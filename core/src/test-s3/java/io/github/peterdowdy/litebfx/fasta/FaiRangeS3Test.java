package io.github.peterdowdy.litebfx.fasta;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for FASTA/FAI reads against MinIO (S3-compatible storage).
 *
 * <p>Verifies that FAI-guided per-contig reads issue HTTP Range requests, seeking to
 * the contig's byte offset in the FASTA file rather than streaming from the start.
 * Activated via the {@code s3-integration} Maven profile.
 *
 * <p>Uses a synthetic 5-contig FASTA ({@code chr1}–{@code chr5}, 300 bases each) so
 * that reading a single contig transfers a fraction of the full file.
 */
class FaiRangeS3Test {

    static final int   CONTIG_COUNT   = 5;
    static final int   BASES_PER_LINE = 60;
    static final int   CONTIG_LENGTH  = 300;
    static final String QUERY_CONTIG  = "chr3";

    @TempDir
    static Path tempDir;

    static SparkSession spark;
    static AmazonS3 s3Client;
    static String bucket;
    static String fastaS3Path;
    static String faiS3Path;
    static long fastaFileSize;

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

        Path faPath = generateFasta(tempDir);
        File faFile  = faPath.toFile();
        File faiFile = faPath.resolveSibling(faPath.getFileName() + ".fai").toFile();
        assertTrue(faiFile.exists(), "FAI not created by FastaSequenceIndexCreator");
        fastaFileSize = faFile.length();

        s3Client.putObject(bucket, "fai-test/multi.fa",     faFile);
        s3Client.putObject(bucket, "fai-test/multi.fa.fai", faiFile);

        fastaS3Path = "s3a://" + bucket + "/fai-test/multi.fa";
        faiS3Path   = "s3a://" + bucket + "/fai-test/multi.fa.fai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("FaiRangeS3Test")
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
                s3Client.deleteObject(bucket, "fai-test/multi.fa");
                s3Client.deleteObject(bucket, "fai-test/multi.fa.fai");
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void indexedRead_contigCount() {
        long count = spark.read().format("fasta")
            .option("indexPath", faiS3Path)
            .load(fastaS3Path)
            .count();
        assertEquals(CONTIG_COUNT, count);
    }

    @Test
    void indexedRead_contigLengthCorrect() {
        Row row = spark.read().format("fasta")
            .option("indexPath", faiS3Path)
            .load(fastaS3Path)
            .filter("name = '" + QUERY_CONTIG + "'")
            .first();
        assertEquals(CONTIG_LENGTH, row.getLong(row.fieldIndex("length")));
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    //
    // The FAI index maps each contig to a byte offset in the FASTA file.
    // FastaPartitionReader calls IndexedFastaSequenceFile.getSequence(name),
    // which seeks to that offset and reads exactly `length` bytes.  With S3A
    // readahead disabled, this becomes a single targeted Range request.
    // Reading one 300-base contig should transfer far fewer bytes than the full
    // ~1900-byte FASTA file.
    // -------------------------------------------------------------------------

    @Test
    void indexedContigRead_readsFewerBytesThanFileSize() {
        long before = s3aBytesRead();
        spark.read().format("fasta")
            .option("indexPath", faiS3Path)
            .load(fastaS3Path)
            .filter("name = '" + QUERY_CONTIG + "'")
            .count();
        long contigBytes = s3aBytesRead() - before;

        // One 300-base contig (plus its FAI entry) should be much less than the whole file
        assertTrue(contigBytes < fastaFileSize,
            String.format(
                "Single-contig read transferred %d bytes; total FASTA file is %d bytes. " +
                "Expected FAI Range seek to contig offset, not a full-file read.",
                contigBytes, fastaFileSize));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes a synthetic FASTA with {@value #CONTIG_COUNT} contigs named
     * {@code chr1}–{@code chr5}, each {@value #CONTIG_LENGTH} bases of 'A',
     * and creates its {@code .fai} index via htsjdk.
     */
    private static Path generateFasta(Path dir) throws Exception {
        Path faPath = dir.resolve("multi.fa");
        try (PrintWriter w = new PrintWriter(faPath.toFile())) {
            for (int i = 1; i <= CONTIG_COUNT; i++) {
                w.println(">chr" + i);
                int remaining = CONTIG_LENGTH;
                while (remaining > 0) {
                    int lineLen = Math.min(BASES_PER_LINE, remaining);
                    w.println("A".repeat(lineLen));
                    remaining -= lineLen;
                }
            }
        }
        FastaSequenceIndexCreator.create(faPath, true);
        return faPath;
    }

    private static long s3aBytesRead() {
        return FileSystem.getAllStatistics().stream()
            .filter(s -> "s3a".equals(s.getScheme()))
            .mapToLong(FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
