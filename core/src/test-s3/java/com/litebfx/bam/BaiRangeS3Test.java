package com.litebfx.bam;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for BAM/BAI reads against MinIO (S3-compatible storage).
 *
 * <p>Verifies that BAI-guided region queries issue HTTP Range requests, transferring
 * fewer bytes than a full-file scan. Activated via the {@code s3-integration} Maven profile.
 *
 * <p>Skipped automatically when {@code s3.endpoint} is not set.
 *
 * <h3>Fixture</h3>
 * A synthetic BAM is generated in {@code setUp()} with two chromosomes:
 * <ul>
 *   <li>{@value #CHR1_NAME}: {@value #CHR1_READS} records — a small fraction of the file.</li>
 *   <li>{@value #CHR2_NAME}: {@value #CHR2_READS} records — the bulk of the file.</li>
 * </ul>
 * With reads long enough ({@value #READ_LENGTH} bp) that the two chromosomes occupy
 * distinct BGZF blocks, a BAI-guided query for {@value #CHR1_NAME} reads far fewer
 * bytes than a full scan.
 */
class BaiRangeS3Test {

    static final String CHR1_NAME   = "chr1";
    static final String CHR2_NAME   = "chr2";
    static final int    CHR1_READS  = 100;
    static final int    CHR2_READS  = 900;
    static final int    TOTAL_READS = CHR1_READS + CHR2_READS;
    /** Read length chosen so that each chromosome spans at least one full BGZF block. */
    static final int    READ_LENGTH = 300;

    static SparkSession spark;
    static AmazonS3 s3Client;
    static String bucket;
    static String bamS3Path;
    static String baiS3Path;
    static long bamFileSize;
    static long baiFileSize;
    static Path tempDir;

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

        tempDir = Files.createTempDirectory("bai-s3-test");
        File bamFile = generateTestBam(tempDir);
        // htsjdk may create the BAI as test.bam.bai or test.bai depending on version.
        // Scan the temp dir to find it rather than hard-coding the name.
        File baiFile;
        try (var stream = Files.list(tempDir)) {
            baiFile = stream.map(Path::toFile)
                            .filter(f -> f.getName().endsWith(".bai"))
                            .findFirst()
                            .orElseThrow(() -> new AssertionError(
                                    "htsjdk did not create a BAI file alongside " + bamFile));
        }
        bamFileSize = bamFile.length();
        baiFileSize = baiFile.length();

        s3Client.putObject(bucket, "bai-test/test.bam",     bamFile);
        s3Client.putObject(bucket, "bai-test/test.bam.bai", baiFile);

        bamS3Path = "s3a://" + bucket + "/bai-test/test.bam";
        baiS3Path = "s3a://" + bucket + "/bai-test/test.bam.bai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("BaiRangeS3Test")
            .config("spark.ui.enabled",                              "false")
            .config("spark.sql.shuffle.partitions",                  "1")
            .config("spark.hadoop.fs.s3a.endpoint",                  endpoint)
            .config("spark.hadoop.fs.s3a.access.key",                ak)
            .config("spark.hadoop.fs.s3a.secret.key",                sk)
            .config("spark.hadoop.fs.s3a.path.style.access",        "true")
            .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
            // Disable readahead so each htsjdk seek maps to its own Range request.
            .config("spark.hadoop.fs.s3a.readahead.range",          "0")
            // Use random-access I/O mode: S3A aborts HTTP connections on seek/close
            // rather than draining remaining bytes. Without this, closing a stream
            // mid-response (e.g., after reading chr1 data) causes S3A to download the
            // remaining chr2 bytes to reuse the keepalive connection, inflating the byte
            // count and defeating the BAI range-request savings assertion.
            .config("spark.hadoop.fs.s3a.input.fadvise",            "random")
            .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
        if (s3Client != null) {
            try {
                s3Client.deleteObject(bucket, "bai-test/test.bam");
                s3Client.deleteObject(bucket, "bai-test/test.bam.bai");
            } catch (Exception ignored) {}
        }
        if (tempDir != null) {
            try (var stream = Files.list(tempDir)) {
                stream.forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
            try { Files.deleteIfExists(tempDir); } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void fullScan_correctCount() {
        long count = spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamS3Path)
            .count();
        assertEquals(TOTAL_READS, count);
    }

    @Test
    void regionQuery_correctCount() {
        long count = spark.read().format("bam")
            .option("indexPath", baiS3Path)
            .load(bamS3Path)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        assertEquals(CHR1_READS, count);
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    //
    // CHR1 has CHR1_READS records, CHR2 has CHR2_READS records, and the reads
    // are long enough (READ_LENGTH bp) that each chromosome occupies distinct
    // BGZF blocks.  A BAI-guided query for CHR1 therefore fetches only CHR1's
    // BGZF chunks plus the BAI and BAM header — a small fraction of the full
    // file — while a full scan streams every byte.
    // -------------------------------------------------------------------------

    @Test
    void regionQuery_readsFewerBytesThanFullScan() {
        // Measure bytes transferred for a BAI-guided region query
        long before = s3aBytesRead();
        spark.read().format("bam")
            .option("indexPath", baiS3Path)
            .load(bamS3Path)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        long regionBytes = s3aBytesRead() - before;

        // Measure bytes transferred for a full-file scan (no index)
        before = s3aBytesRead();
        spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamS3Path)
            .count();
        long fullBytes = s3aBytesRead() - before;

        // Region read (BAI + CHR1 chunks) must be less than full scan
        assertTrue(regionBytes < fullBytes,
            String.format(
                "BAI region query (%s, %d reads) transferred %d bytes but full scan " +
                "(%d reads) transferred %d bytes. " +
                "Expected region < full — BAI Range requests not firing correctly.",
                CHR1_NAME, CHR1_READS, regionBytes, TOTAL_READS, fullBytes));

        // Region read must also be less than the raw file size
        assertTrue(regionBytes < bamFileSize,
            String.format(
                "BAI region query transferred %d bytes >= file size %d bytes. " +
                "Expected a targeted partial read.",
                regionBytes, bamFileSize));
    }

    // -------------------------------------------------------------------------
    // Fixture generation
    // -------------------------------------------------------------------------

    /**
     * Generates a coordinate-sorted BAM with two chromosomes and a co-located BAI.
     *
     * <p>Each read uses a deterministic pseudo-random sequence so that consecutive
     * reads are not compressible together — this ensures the two chromosomes occupy
     * distinct BGZF blocks even after BGZF compression.  With repetitive sequences
     * (e.g. {@code "A".repeat(n)}) the entire file can collapse into one or two BGZF
     * blocks, making BAI-guided savings invisible.
     */
    private static File generateTestBam(Path dir) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(CHR1_NAME, 100_000_000));
        header.addSequence(new SAMSequenceRecord(CHR2_NAME, 100_000_000));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        String cigar     = READ_LENGTH + "M";
        String qualities = "I".repeat(READ_LENGTH);

        File bamFile = dir.resolve("test.bam").toFile();
        SAMFileWriterFactory factory = new SAMFileWriterFactory().setCreateIndex(true);

        try (SAMFileWriter writer = factory.makeBAMWriter(header, true, bamFile)) {
            for (int i = 0; i < CHR1_READS; i++) {
                writer.addAlignment(makeRecord(header, "chr1_read" + (i + 1), 0,
                        (i + 1) * 1000, cigar, pseudoRandomSeq(i, 0), qualities));
            }
            for (int i = 0; i < CHR2_READS; i++) {
                writer.addAlignment(makeRecord(header, "chr2_read" + (i + 1), 1,
                        (i + 1) * 1000, cigar, pseudoRandomSeq(i, 1), qualities));
            }
        }
        return bamFile;
    }

    /**
     * Returns a deterministic pseudo-random DNA sequence of {@value #READ_LENGTH} bases.
     * Using an LCG ensures different reads produce different sequences, keeping BGZF
     * compression ratios low and the resulting BAM large enough that chromosomes land
     * in distinct compressed blocks.
     */
    private static String pseudoRandomSeq(int readIdx, int chromIdx) {
        char[] bases = "ACGT".toCharArray();
        int seed = readIdx * 1_013 + chromIdx * 7_919;
        char[] seq = new char[READ_LENGTH];
        for (int i = 0; i < READ_LENGTH; i++) {
            seed = seed * 1_664_525 + 1_013_904_223;
            seq[i] = bases[(seed >>> 16) & 3];
        }
        return new String(seq);
    }

    private static SAMRecord makeRecord(SAMFileHeader header, String name, int refIdx,
                                        int start, String cigar, String seq, String qual) {
        SAMRecord r = new SAMRecord(header);
        r.setReadName(name);
        r.setFlags(0);
        r.setReferenceIndex(refIdx);
        r.setAlignmentStart(start);
        r.setMappingQuality(60);
        r.setCigarString(cigar);
        r.setReadString(seq);
        r.setBaseQualityString(qual);
        r.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
        r.setMateAlignmentStart(0);
        r.setInferredInsertSize(0);
        return r;
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    /** Returns the cumulative S3A bytes-read counter across all threads. */
    private static long s3aBytesRead() {
        return FileSystem.getAllStatistics().stream()
            .filter(s -> "s3a".equals(s.getScheme()))
            .mapToLong(FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
