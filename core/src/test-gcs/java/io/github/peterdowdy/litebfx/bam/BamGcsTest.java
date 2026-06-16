package io.github.peterdowdy.litebfx.bam;

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
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for BAM/BAI reads against fake-gcs-server (GCS emulator).
 *
 * <p>Verifies that BAI-guided region queries issue HTTP Range requests, transferring
 * fewer bytes than a full-file scan. Activated via the {@code gcs-integration} Maven profile.
 *
 * <p>Uses a synthetic two-chromosome BAM (chr1: 100 reads, chr2: 900 reads) with reads
 * long enough ({@value #READ_LENGTH} bp) to occupy distinct BGZF blocks per chromosome.
 *
 * <p>Skipped automatically when {@code gcs.endpoint} is not set.
 */
class BamGcsTest {

    static final String CHR1_NAME   = "chr1";
    static final String CHR2_NAME   = "chr2";
    static final int    CHR1_READS  = 100;
    static final int    CHR2_READS  = 3900;
    static final int    TOTAL_READS = CHR1_READS + CHR2_READS;
    static final int    READ_LENGTH = 300;

    @TempDir
    static Path tempDir;

    static HttpServer fakeMetadataServer;
    static SparkSession spark;
    static String endpoint;
    static String bucket;
    static String bamGcsPath;
    static String baiGcsPath;
    static long bamFileSize;

    @BeforeAll
    static void setUp() throws Exception {
        // Start a mock GCE metadata server so the GCS connector gets a fake Bearer token
        // instead of timing out trying to reach 169.254.169.254.
        // fake-gcs-server accepts any Bearer token without validation.
        startFakeMetadataServer();

        endpoint = System.getProperty("gcs.endpoint", "");
        assumeTrue(!endpoint.isBlank(),
            "gcs.endpoint not set — skipping GCS range-access tests " +
            "(run with -Pgcs-integration -Dgcs.endpoint=http://localhost:4443)");

        bucket = System.getProperty("gcs.bucket", "test-bucket");
        String project = System.getProperty("gcs.project", "test-project");

        createBucket(endpoint, bucket, project);

        File bamFile = generateTestBam(tempDir);
        File baiFile = findBai(tempDir, bamFile);
        bamFileSize = bamFile.length();

        uploadObject(endpoint, bucket, "bam-test/test.bam",     bamFile);
        uploadObject(endpoint, bucket, "bam-test/test.bam.bai", baiFile);

        bamGcsPath = "gs://" + bucket + "/bam-test/test.bam";
        baiGcsPath = "gs://" + bucket + "/bam-test/test.bam.bai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("BamGcsTest")
            .config("spark.ui.enabled",                                     "false")
            .config("spark.sql.shuffle.partitions",                          "1")
            .config("spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.fs.gs.storage.root.url",                  endpoint)
            .config("spark.hadoop.fs.gs.auth.type",                         "UNAUTHENTICATED")
            .config("spark.hadoop.google.cloud.auth.null.enable",           "true")
            .getOrCreate();

        // Apply auth settings directly to the Hadoop conf so the GCS FileSystem picks
        // them up on first access, in addition to the spark.hadoop.* prefix path.
        org.apache.hadoop.conf.Configuration hc = spark.sparkContext().hadoopConfiguration();
        hc.set("fs.gs.auth.type",                "UNAUTHENTICATED");
        hc.set("google.cloud.auth.null.enable",  "true");
        hc.set("fs.gs.storage.root.url",         endpoint);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
        if (fakeMetadataServer != null) fakeMetadataServer.stop(0);
        try { FileSystem.closeAll(); } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void fullScan_correctCount() {
        long count = spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamGcsPath)
            .count();
        assertEquals(TOTAL_READS, count);
    }

    @Test
    void regionQuery_correctCount() {
        long count = spark.read().format("bam")
            .option("indexPath", baiGcsPath)
            .load(bamGcsPath)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        assertEquals(CHR1_READS, count);
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    // -------------------------------------------------------------------------

    @Test
    void regionQuery_readsFewerBytesThanFullScan() {
        long before = gcsBytesRead();
        spark.read().format("bam")
            .option("indexPath", baiGcsPath)
            .load(bamGcsPath)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        long regionBytes = gcsBytesRead() - before;

        before = gcsBytesRead();
        spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamGcsPath)
            .count();
        long fullBytes = gcsBytesRead() - before;

        assertTrue(regionBytes < fullBytes,
            String.format(
                "BAI region query (%s, %d reads) transferred %d bytes but full scan " +
                "(%d reads) transferred %d bytes. Expected region < full.",
                CHR1_NAME, CHR1_READS, regionBytes, TOTAL_READS, fullBytes));

        assertTrue(regionBytes < bamFileSize,
            String.format(
                "BAI region query transferred %d bytes >= file size %d bytes.",
                regionBytes, bamFileSize));
    }

    // -------------------------------------------------------------------------
    // Fixture helpers
    // -------------------------------------------------------------------------

    private static File generateTestBam(Path dir) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(CHR1_NAME, 100_000_000));
        header.addSequence(new SAMSequenceRecord(CHR2_NAME, 100_000_000));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        String cigar     = READ_LENGTH + "M";
        String qualities = "I".repeat(READ_LENGTH);

        File bamFile = dir.resolve("test.bam").toFile();
        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .setCreateIndex(true)
                .makeBAMWriter(header, true, bamFile)) {
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

    private static File findBai(Path dir, File bamFile) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.map(Path::toFile)
                         .filter(f -> f.getName().endsWith(".bai"))
                         .findFirst()
                         .orElseThrow(() -> new AssertionError(
                                 "htsjdk did not create a BAI file alongside " + bamFile));
        }
    }

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
    // GCS REST upload helpers (no GCS client library needed)
    // -------------------------------------------------------------------------

    private static void createBucket(String ep, String bkt, String project) throws IOException {
        URL url = URI.create(ep + "/storage/v1/b?project=" + project).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        byte[] body = ("{\"name\":\"" + bkt + "\"}").getBytes();
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body);
        }
        int code = conn.getResponseCode();
        // 200 = created, 409 = already exists — both are fine
        if (code != 200 && code != 409) {
            throw new IOException("Failed to create GCS bucket, HTTP " + code);
        }
    }

    private static void uploadObject(String ep, String bkt, String key, File file) throws IOException {
        String encodedKey = key.replace("/", "%2F");
        URL url = URI.create(ep + "/upload/storage/v1/b/" + bkt
                + "/o?uploadType=media&name=" + encodedKey).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(Files.readAllBytes(file.toPath()));
        }
        int code = conn.getResponseCode();
        if (code != 200) {
            throw new IOException("Failed to upload " + key + " to GCS, HTTP " + code);
        }
    }

    // -------------------------------------------------------------------------
    // Fake GCE metadata server (avoids 169.254.169.254 TCP timeout)
    //
    // The GCS connector uses GCE_METADATA_HOST (env var, set to 127.0.0.1:21999 via
    // surefire) to find the metadata server. This mock returns a fake Bearer token.
    // fake-gcs-server accepts any Bearer token without validation.
    // -------------------------------------------------------------------------

    private static void startFakeMetadataServer() throws IOException {
        fakeMetadataServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 21999), 0);
        byte[] fakeToken = ("{\"access_token\":\"fake-gcs-token\","
                + "\"expires_in\":3600,\"token_type\":\"Bearer\"}")
                .getBytes(StandardCharsets.UTF_8);
        fakeMetadataServer.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, fakeToken.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(fakeToken);
            }
        });
        fakeMetadataServer.start();
    }

    // -------------------------------------------------------------------------
    // Metric helper
    // -------------------------------------------------------------------------

    private static long gcsBytesRead() {
        return FileSystem.getAllStatistics().stream()
            .filter(s -> "gs".equals(s.getScheme()))
            .mapToLong(FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
