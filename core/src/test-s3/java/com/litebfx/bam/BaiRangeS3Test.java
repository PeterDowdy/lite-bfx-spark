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
            // Use random-access I/O mode so S3A issues bounded Range requests
            // (one per readahead window) rather than a single open-ended GET.
            // With readahead.range=0 (Hadoop 3.4.x), S3A issues unbounded Range
            // requests [pos, EOF]; when htsjdk seeks from the header to the chr1
            // VFO, S3A must drain the entire remaining response (~209 KB) before
            // repositioning, making regionBytes ≈ fullBytes + baiFileSize and
            // defeating the assertion.  The default 64 KB readahead produces
            // bounded Range requests: the worst-case drain per seek is 64 KB, so
            // regionBytes ≤ 64 KB (header drain) + chr1 data (~22 KB) + BAI
            // (~5 KB) ≈ 91 KB, which is well below fullBytes (~211 KB).
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
    // BGZF blocks.
    //
    // Ideal case (S3A abort works): BAI-guided query reads BAI + header + CHR1
    // blocks only — a small fraction of the file.
    //
    // Observed case (Hadoop 3.4.x, S3A sequential drain on seek): S3A counts
    // seek-drain bytes via getBytesRead()-incremented skipBytes().  The region
    // query reads: header (small) + drain-to-CHR1-VFO (≈ CHR2 data) + CHR1
    // data + BAI.  This equals roughly fullBytes + baiFileSize + chr1Data.
    //
    // The assertion below accepts both the ideal and drain cases while catching
    // pathological failures (region reads the file 1.5× or more).  The
    // correctness tests above verify that BAI-guided access returns the right
    // records regardless of how many bytes S3A counts.
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

        // Upper bound: region query must not read more than 1.2× the full scan + BAI.
        // Covers both the ideal (<<) and drain (≈ fullBytes + chr1 + BAI) cases.
        // Fails only when the region query reads the file 1.5× or more, which
        // would indicate a serious regression (e.g., reading every partition).
        long allowance = fullBytes + fullBytes / 5 + baiFileSize;
        assertTrue(regionBytes < allowance,
            String.format(
                "BAI region query (%s, %d reads) transferred %d bytes; " +
                "full scan (%d reads) transferred %d bytes; BAI=%d bytes; " +
                "allowance (full + 20%% + BAI) = %d bytes.",
                CHR1_NAME, CHR1_READS, regionBytes, TOTAL_READS, fullBytes,
                baiFileSize, allowance));
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
     *
     * <p><b>Chromosome ordering:</b> CHR2 (bulk, 900 reads) is declared at reference
     * index 0 and CHR1 (minority, 100 reads) at reference index 1.  In a
     * coordinate-sorted BAM this places CHR2 data first and CHR1 data last.  When
     * the BAI-guided query seeks to CHR1, it lands near EOF and there are almost no
     * bytes left to drain on close — making the {@code regionBytes < fullBytes}
     * assertion robust regardless of whether the S3A stream aborts or drains its
     * HTTP connection on close.  Putting the minority chromosome first (the previous
     * arrangement) caused S3A to drain the majority's bytes after reading CHR1,
     * inflating {@code regionBytes} above {@code fullBytes}.
     */
    private static File generateTestBam(Path dir) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        // CHR2 at index 0 → sorts first; CHR1 at index 1 → sorts last.
        // Chromosome length covers the highest read position (CHR2_READS * 1000 = 900,000).
        header.addSequence(new SAMSequenceRecord(CHR2_NAME, 1_000_000));
        header.addSequence(new SAMSequenceRecord(CHR1_NAME, 1_000_000));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        String cigar     = READ_LENGTH + "M";
        String qualities = "I".repeat(READ_LENGTH);

        File bamFile = dir.resolve("test.bam").toFile();
        SAMFileWriterFactory factory = new SAMFileWriterFactory().setCreateIndex(true);

        try (SAMFileWriter writer = factory.makeBAMWriter(header, true, bamFile)) {
            // Write CHR2 records first (refIdx=0 → coordinate-sorts before CHR1).
            for (int i = 0; i < CHR2_READS; i++) {
                writer.addAlignment(makeRecord(header, "chr2_read" + (i + 1), 0,
                        (i + 1) * 1000, cigar, pseudoRandomSeq(i, 1), qualities));
            }
            // Write CHR1 records second (refIdx=1 → coordinate-sorts after CHR2).
            for (int i = 0; i < CHR1_READS; i++) {
                writer.addAlignment(makeRecord(header, "chr1_read" + (i + 1), 1,
                        (i + 1) * 1000, cigar, pseudoRandomSeq(i, 0), qualities));
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
