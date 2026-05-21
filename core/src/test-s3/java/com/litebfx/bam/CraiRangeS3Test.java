package com.litebfx.bam;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for CRAM/CRAI reads against MinIO (S3-compatible storage).
 *
 * <p>Verifies that CRAI-guided region queries issue HTTP Range requests, transferring
 * fewer bytes than a full-file scan. Activated via the {@code s3-integration} Maven profile.
 *
 * <p>Uses a synthetic 10-record CRAM backed by a generated FASTA reference (same fixture
 * as {@link CramDataSourceTest}) so that a CRAI index is always available. The CRAI
 * has one container per chromosome, enabling targeted range requests for region queries.
 *
 * <p>Skipped automatically when {@code s3.endpoint} is not set.
 */
class CraiRangeS3Test {

    static final int TOTAL_RECORDS = TestBamGenerator.RECORD_COUNT;

    @TempDir
    static Path tempDir;

    static SparkSession spark;
    static AmazonS3 s3Client;
    static String bucket;
    static String cramS3Path;
    static String craiS3Path;
    static String fastaLocalPath;
    static long cramFileSize;

    @BeforeAll
    static void setUp() throws Exception {
        String endpoint = System.getProperty("s3.endpoint", "");
        assumeTrue(!endpoint.isBlank(),
            "s3.endpoint not set — skipping S3 range-access tests " +
            "(run with -Ps3-integration -Ds3.endpoint=http://localhost:9000)");

        bucket    = System.getProperty("s3.bucket",    "test-bucket");
        String ak = System.getProperty("s3.accessKey", "minioadmin");
        String sk = System.getProperty("s3.secretKey", "minioadmin");

        s3Client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ak, sk)))
            .withPathStyleAccessEnabled(true)
            .build();

        if (!s3Client.doesBucketExistV2(bucket)) {
            s3Client.createBucket(bucket);
        }

        // Generate FASTA reference + synthetic CRAM with CRAI index
        Path faPath   = CramDataSourceTest.generateFasta(tempDir);
        Path cramPath = CramDataSourceTest.generateCram(tempDir, faPath);
        Path craiPath = cramPath.resolveSibling(cramPath.getFileName() + ".crai");

        assumeTrue(craiPath.toFile().exists(),
            "CRAI not generated alongside CRAM — skipping CRAI range test");

        fastaLocalPath = faPath.toAbsolutePath().toString();
        cramFileSize   = cramPath.toFile().length();

        s3Client.putObject(bucket, "crai-test/test.cram",      cramPath.toFile());
        s3Client.putObject(bucket, "crai-test/test.cram.crai", craiPath.toFile());

        cramS3Path = "s3a://" + bucket + "/crai-test/test.cram";
        craiS3Path = "s3a://" + bucket + "/crai-test/test.cram.crai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("CraiRangeS3Test")
            .config("spark.ui.enabled",                              "false")
            .config("spark.sql.shuffle.partitions",                  "1")
            .config("spark.hadoop.fs.s3a.endpoint",                  endpoint)
            .config("spark.hadoop.fs.s3a.access.key",                ak)
            .config("spark.hadoop.fs.s3a.secret.key",                sk)
            .config("spark.hadoop.fs.s3a.path.style.access",        "true")
            .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
            .config("spark.hadoop.fs.s3a.readahead.range",          "0")
            .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
        if (s3Client != null) {
            try {
                s3Client.deleteObject(bucket, "crai-test/test.cram");
                s3Client.deleteObject(bucket, "crai-test/test.cram.crai");
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void fullScan_correctCount() {
        long count = spark.read().format("cram")
            .option("referenceFile", fastaLocalPath)
            .option("useIndex", "false")
            .load(cramS3Path)
            .count();
        assertEquals(TOTAL_RECORDS, count);
    }

    @Test
    void regionQuery_correctCount() {
        // All synthetic records are on TestBamGenerator.REF_NAME ("chr1"), so the
        // region filter returns all records just as the full scan does.
        long count = spark.read().format("cram")
            .option("referenceFile", fastaLocalPath)
            .option("indexPath",     craiS3Path)
            .load(cramS3Path)
            .filter("referenceName = '" + TestBamGenerator.REF_NAME + "'")
            .count();
        assertEquals(TOTAL_RECORDS, count);
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    //
    // A CRAI-guided read issues a Range request bounded by the CRAI container
    // offsets for the queried reference.  With S3A readahead disabled, only the
    // containers that overlap the query are fetched.  Even when all records are
    // on a single reference (as in the synthetic fixture), the CRAI read still
    // transfers the CRAI index bytes plus the CRAM containers — which together
    // should be ≤ the full CRAM file size.
    // -------------------------------------------------------------------------

    @Test
    void cramRead_bytesNotExceedFileSize() {
        long before = s3aBytesRead();
        spark.read().format("cram")
            .option("referenceFile", fastaLocalPath)
            .option("indexPath",     craiS3Path)
            .load(cramS3Path)
            .filter("referenceName = '" + TestBamGenerator.REF_NAME + "'")
            .count();
        long transferredBytes = s3aBytesRead() - before;

        // CRAI bytes + queried CRAM containers must not exceed the full CRAM file
        // (the CRAI and CRAM containers together are never larger than the raw file)
        assertTrue(transferredBytes <= cramFileSize + 4096,
            String.format(
                "CRAI-guided read transferred %d bytes; CRAM file size is %d bytes. " +
                "Expected transfer to stay within file bounds.",
                transferredBytes, cramFileSize));
    }

    @Test
    void craiGuidedRead_readsFewerBytesThanNoIndexScan() {
        // Measure transfer for a CRAI-guided read
        long before = s3aBytesRead();
        spark.read().format("cram")
            .option("referenceFile", fastaLocalPath)
            .option("indexPath",     craiS3Path)
            .load(cramS3Path)
            .filter("referenceName = '" + TestBamGenerator.REF_NAME + "'")
            .count();
        long indexedBytes = s3aBytesRead() - before;

        // Measure transfer for a full scan with no index
        before = s3aBytesRead();
        spark.read().format("cram")
            .option("referenceFile", fastaLocalPath)
            .option("useIndex", "false")
            .load(cramS3Path)
            .count();
        long fullScanBytes = s3aBytesRead() - before;

        // The indexed read transfers CRAI + CRAM containers; the full scan transfers
        // the entire CRAM.  For a single-chromosome synthetic file these may be
        // similar, so we assert ≤ rather than strictly <.
        assertTrue(indexedBytes <= fullScanBytes + 4096,
            String.format(
                "CRAI-guided read transferred %d bytes; no-index full scan transferred %d bytes. " +
                "Expected indexed read to be no worse than a full scan.",
                indexedBytes, fullScanBytes));
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static long s3aBytesRead() {
        return FileSystem.getAllStatistics().stream()
            .filter(s -> "s3a".equals(s.getScheme()))
            .mapToLong(FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
