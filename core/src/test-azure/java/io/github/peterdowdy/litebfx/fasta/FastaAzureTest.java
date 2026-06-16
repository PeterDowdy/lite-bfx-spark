package io.github.peterdowdy.litebfx.fasta;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for FASTA/FAI reads against Azurite (Azure Blob emulator).
 *
 * <p>Verifies that FAI-guided per-contig reads issue HTTP Range requests, seeking directly
 * to the contig's byte offset rather than streaming the entire file. Activated via the
 * {@code azure-integration} Maven profile.
 *
 * <p>Uses a synthetic 5-contig FASTA ({@code chr1}–{@code chr5}, 300 bases each).
 *
 * <p>Skipped automatically if {@code devstoreaccount1.dfs.core.windows.net} is not resolvable.
 */
class FastaAzureTest {

    static final int    CONTIG_COUNT   = 5;
    static final int    BASES_PER_LINE = 60;
    static final int    CONTIG_LENGTH  = 300;
    static final String QUERY_CONTIG   = "chr3";

    @TempDir
    static java.nio.file.Path tempDir;

    static SparkSession spark;
    static String account;
    static String container;
    static String accountKey;
    static String fastaAzurePath;
    static String faiAzurePath;
    static long fastaFileSize;

    @BeforeAll
    static void setUp() throws Exception {
        account    = System.getProperty("azure.account",   "devstoreaccount1");
        container  = System.getProperty("azure.container", "test-container");
        accountKey = System.getProperty("azure.key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="); // gitleaks:allow

        String blobHost = account + ".blob.core.windows.net";
        try {
            java.net.InetAddress.getByName(blobHost);
        } catch (java.net.UnknownHostException e) {
            assumeTrue(false,
                "Cannot resolve " + blobHost + " — skipping Azure tests. " +
                "In Docker, ensure the azurite service exposes network aliases.");
        }

        java.nio.file.Path faPath = generateFasta(tempDir);
        File faFile  = faPath.toFile();
        File faiFile = faPath.resolveSibling(faPath.getFileName() + ".fai").toFile();
        assertTrue(faiFile.exists(), "FAI not created by FastaSequenceIndexCreator");
        fastaFileSize = faFile.length();

        fastaAzurePath = "wasb://" + container + "@" + blobHost + "/fasta-test/multi.fa";
        faiAzurePath   = "wasb://" + container + "@" + blobHost + "/fasta-test/multi.fa.fai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("FastaAzureTest")
            .config("spark.ui.enabled",                                              "false")
            .config("spark.sql.shuffle.partitions",                                   "1")
            .config("spark.hadoop.fs.azure.account.key." + blobHost,                 accountKey)
            .config("spark.hadoop.fs.azure.always.use.https",                        "false")
            .getOrCreate();

        String connStr = azuriteConnStr(blobHost, account, accountKey);
        createContainerSdk(connStr, container);
        uploadBlobSdk(connStr, container, "fasta-test/multi.fa",     faFile);
        uploadBlobSdk(connStr, container, "fasta-test/multi.fa.fai", faiFile);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void indexedRead_contigCount() {
        long count = spark.read().format("fasta")
            .option("indexPath", faiAzurePath)
            .load(fastaAzurePath)
            .count();
        assertEquals(CONTIG_COUNT, count);
    }

    @Test
    void indexedRead_contigLengthCorrect() {
        Row row = spark.read().format("fasta")
            .option("indexPath", faiAzurePath)
            .load(fastaAzurePath)
            .filter("name = '" + QUERY_CONTIG + "'")
            .first();
        assertEquals(CONTIG_LENGTH, row.getLong(row.fieldIndex("length")));
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    // -------------------------------------------------------------------------

    @Test
    void indexedContigRead_readsFewerBytesThanFileSize() {
        long before = azureBytesRead();
        spark.read().format("fasta")
            .option("indexPath", faiAzurePath)
            .load(fastaAzurePath)
            .filter("name = '" + QUERY_CONTIG + "'")
            .count();
        long contigBytes = azureBytesRead() - before;

        assertTrue(contigBytes < fastaFileSize,
            String.format(
                "Single-contig read transferred %d bytes; total FASTA file is %d bytes. " +
                "Expected FAI Range seek to contig offset, not a full-file read.",
                contigBytes, fastaFileSize));
    }

    // -------------------------------------------------------------------------
    // Fixture helpers
    // -------------------------------------------------------------------------

    private static java.nio.file.Path generateFasta(java.nio.file.Path dir) throws Exception {
        java.nio.file.Path faPath = dir.resolve("multi.fa");
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
    // Azure upload helpers
    // -------------------------------------------------------------------------

    private static String azuriteConnStr(String blobHost, String acct, String key) {
        return "DefaultEndpointsProtocol=http;AccountName=" + acct
                + ";AccountKey=" + key
                + ";BlobEndpoint=http://" + blobHost + ";";
    }

    private static void createContainerSdk(String connStr, String ctr) throws Exception {
        CloudStorageAccount acct = CloudStorageAccount.parse(connStr);
        CloudBlobContainer container = acct.createCloudBlobClient().getContainerReference(ctr);
        container.createIfNotExists();
    }

    private static void uploadBlobSdk(String connStr, String ctr, String blobName, File file)
            throws Exception {
        CloudStorageAccount acct = CloudStorageAccount.parse(connStr);
        CloudBlobContainer container = acct.createCloudBlobClient().getContainerReference(ctr);
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);
        blob.uploadFromFile(file.getAbsolutePath());
    }

    // -------------------------------------------------------------------------
    // Metric helper
    // -------------------------------------------------------------------------

    private static long azureBytesRead() {
        return org.apache.hadoop.fs.FileSystem.getAllStatistics().stream()
            .filter(s -> "wasb".equals(s.getScheme()) || "wasbs".equals(s.getScheme()))
            .mapToLong(org.apache.hadoop.fs.FileSystem.Statistics::getBytesRead)
            .sum();
    }
}
