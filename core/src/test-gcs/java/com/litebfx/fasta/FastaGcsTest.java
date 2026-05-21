package com.litebfx.fasta;

import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
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
 * Range-access integration tests for FASTA/FAI reads against fake-gcs-server (GCS emulator).
 *
 * <p>Verifies that FAI-guided per-contig reads issue HTTP Range requests, seeking directly
 * to the contig's byte offset rather than streaming the entire file. Activated via the
 * {@code gcs-integration} Maven profile.
 *
 * <p>Uses a synthetic 5-contig FASTA ({@code chr1}–{@code chr5}, 300 bases each).
 *
 * <p>Skipped automatically when {@code gcs.endpoint} is not set.
 */
class FastaGcsTest {

    static final int    CONTIG_COUNT   = 5;
    static final int    BASES_PER_LINE = 60;
    static final int    CONTIG_LENGTH  = 300;
    static final String QUERY_CONTIG   = "chr3";

    @TempDir
    static Path tempDir;

    static HttpServer fakeMetadataServer;
    static SparkSession spark;
    static String endpoint;
    static String bucket;
    static String fastaGcsPath;
    static String faiGcsPath;
    static long fastaFileSize;

    @BeforeAll
    static void setUp() throws Exception {
        startFakeMetadataServer();

        endpoint = System.getProperty("gcs.endpoint", "");
        assumeTrue(!endpoint.isBlank(),
            "gcs.endpoint not set — skipping GCS range-access tests " +
            "(run with -Pgcs-integration -Dgcs.endpoint=http://localhost:4443)");

        bucket = System.getProperty("gcs.bucket", "test-bucket");
        String project = System.getProperty("gcs.project", "test-project");

        createBucket(endpoint, bucket, project);

        Path faPath  = generateFasta(tempDir);
        File faFile  = faPath.toFile();
        File faiFile = faPath.resolveSibling(faPath.getFileName() + ".fai").toFile();
        assertTrue(faiFile.exists(), "FAI not created by FastaSequenceIndexCreator");
        fastaFileSize = faFile.length();

        uploadObject(endpoint, bucket, "fasta-test/multi.fa",     faFile);
        uploadObject(endpoint, bucket, "fasta-test/multi.fa.fai", faiFile);

        fastaGcsPath = "gs://" + bucket + "/fasta-test/multi.fa";
        faiGcsPath   = "gs://" + bucket + "/fasta-test/multi.fa.fai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("FastaGcsTest")
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
    void indexedRead_contigCount() {
        long count = spark.read().format("fasta")
            .option("indexPath", faiGcsPath)
            .load(fastaGcsPath)
            .count();
        assertEquals(CONTIG_COUNT, count);
    }

    @Test
    void indexedRead_contigLengthCorrect() {
        Row row = spark.read().format("fasta")
            .option("indexPath", faiGcsPath)
            .load(fastaGcsPath)
            .filter("name = '" + QUERY_CONTIG + "'")
            .first();
        assertEquals(CONTIG_LENGTH, row.getLong(row.fieldIndex("length")));
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    // -------------------------------------------------------------------------

    @Test
    void indexedContigRead_readsFewerBytesThanFileSize() {
        long before = gcsBytesRead();
        spark.read().format("fasta")
            .option("indexPath", faiGcsPath)
            .load(fastaGcsPath)
            .filter("name = '" + QUERY_CONTIG + "'")
            .count();
        long contigBytes = gcsBytesRead() - before;

        assertTrue(contigBytes < fastaFileSize,
            String.format(
                "Single-contig read transferred %d bytes; total FASTA file is %d bytes. " +
                "Expected FAI Range seek to contig offset, not a full-file read.",
                contigBytes, fastaFileSize));
    }

    // -------------------------------------------------------------------------
    // Fixture helpers
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Fake GCE metadata server
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
    // GCS REST upload helpers
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
    // Metric helper
    // -------------------------------------------------------------------------

    private static long gcsBytesRead() {
        return FileSystem.getAllStatistics().stream()
            .filter(s -> "gs".equals(s.getScheme()))
            .mapToLong(FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
