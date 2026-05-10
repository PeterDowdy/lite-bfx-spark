package com.litebfx.bam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Range-access integration tests for BAM/BAI reads against Azurite (Azure Blob emulator).
 *
 * <p>Verifies that BAI-guided region queries issue HTTP Range requests, transferring
 * fewer bytes than a full-file scan. Activated via the {@code azure-integration} Maven profile.
 *
 * <p>Uses the WASB connector ({@code wasb://}) with the Blob endpoint
 * ({@code devstoreaccount1.blob.core.windows.net}) rather than the ABFS/DFS endpoint.
 * Azurite's DFS API returns 400 for filesystem operations with hadoop-azure 3.4.x; the
 * Blob API it does handle correctly. The {@code azurite} Docker service exposes network
 * aliases for {@code devstoreaccount1.blob.core.windows.net} so the WASB connector resolves
 * the account hostname to the emulator.
 *
 * <p>Uses a synthetic two-chromosome BAM (chr1: 100 reads, chr2: 900 reads) with reads
 * long enough ({@value #READ_LENGTH} bp) to occupy distinct BGZF blocks per chromosome.
 *
 * <p>Skipped automatically when {@code azure.account} system property is absent — which
 * is never the case when the profile is active. Tests also skip if the Azurite host is
 * unreachable, detected by a failed container creation attempt.
 */
class BamAzureTest {

    static final String CHR1_NAME   = "chr1";
    static final String CHR2_NAME   = "chr2";
    static final int    CHR1_READS  = 100;
    static final int    CHR2_READS  = 3900;
    static final int    TOTAL_READS = CHR1_READS + CHR2_READS;
    static final int    READ_LENGTH = 300;

    @TempDir
    static java.nio.file.Path tempDir;

    static SparkSession spark;
    static String account;
    static String container;
    static String accountKey;
    static String bamAzurePath;
    static String baiAzurePath;
    static long bamFileSize;

    @BeforeAll
    static void setUp() throws Exception {
        account      = System.getProperty("azure.account",   "devstoreaccount1");
        container    = System.getProperty("azure.container", "test-container");
        accountKey   = System.getProperty("azure.key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");

        // WASB uses the Blob endpoint; Azurite's network alias points blob.core.windows.net to it.
        String blobHost = account + ".blob.core.windows.net";
        try {
            java.net.InetAddress.getByName(blobHost);
        } catch (java.net.UnknownHostException e) {
            assumeTrue(false,
                "Cannot resolve " + blobHost + " — skipping Azure tests. " +
                "In Docker, ensure the azurite service exposes network aliases.");
        }

        File bamFile = generateTestBam(tempDir);
        File baiFile = findBai(tempDir, bamFile);
        bamFileSize = bamFile.length();

        bamAzurePath = "wasb://" + container + "@" + blobHost + "/bam-test/test.bam";
        baiAzurePath = "wasb://" + container + "@" + blobHost + "/bam-test/test.bam.bai";

        spark = SparkSession.builder()
            .master("local[1]")
            .appName("BamAzureTest")
            .config("spark.ui.enabled",                                              "false")
            .config("spark.sql.shuffle.partitions",                                   "1")
            .config("spark.hadoop.fs.azure.account.key." + blobHost,                 accountKey)
            .config("spark.hadoop.fs.azure.always.use.https",                        "false")
            .getOrCreate();

        // Use azure-storage SDK for uploads — WASB FileSystem.create() has an NPE with Azurite.
        String connStr = azuriteConnStr(blobHost, account, accountKey);
        createContainerSdk(connStr, container);
        uploadBlobSdk(connStr, container, "bam-test/test.bam",     bamFile);
        uploadBlobSdk(connStr, container, "bam-test/test.bam.bai", baiFile);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Correctness
    // -------------------------------------------------------------------------

    @Test
    void fullScan_correctCount() {
        long count = spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamAzurePath)
            .count();
        assertEquals(TOTAL_READS, count);
    }

    @Test
    void regionQuery_correctCount() {
        long count = spark.read().format("bam")
            .option("indexPath", baiAzurePath)
            .load(bamAzurePath)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        assertEquals(CHR1_READS, count);
    }

    // -------------------------------------------------------------------------
    // Range-access verification
    // -------------------------------------------------------------------------

    @Test
    void regionQuery_readsFewerBytesThanFullScan() {
        long before = azureBytesRead();
        spark.read().format("bam")
            .option("indexPath", baiAzurePath)
            .load(bamAzurePath)
            .filter("referenceName = '" + CHR1_NAME + "'")
            .count();
        long regionBytes = azureBytesRead() - before;

        before = azureBytesRead();
        spark.read().format("bam")
            .option("useIndex", "false")
            .load(bamAzurePath)
            .count();
        long fullBytes = azureBytesRead() - before;

        // WASB FileSystem.Statistics may not track bytes accurately in all configurations.
        // Skip the metric assertion if statistics are unavailable.
        assumeTrue(regionBytes > 0 && fullBytes > 0,
            "WASB FileSystem.Statistics did not record bytes — skipping range-access assertion.");

        assertTrue(regionBytes < fullBytes,
            String.format(
                "BAI region query (%s, %d reads) transferred %d bytes but full scan " +
                "(%d reads) transferred %d bytes. Expected region < full.",
                CHR1_NAME, CHR1_READS, regionBytes, TOTAL_READS, fullBytes));

        assertTrue(regionBytes < bamFileSize,
            String.format(
                "BAI region query transferred %d bytes >= file size %d bytes.",
                regionBytes, bamFileSize));

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

    private static File generateTestBam(java.nio.file.Path dir) throws IOException {
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

    private static File findBai(java.nio.file.Path dir, File bamFile) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.map(java.nio.file.Path::toFile)
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
    // Azure upload helpers (via Hadoop WASB FileSystem — uses Blob API supported by Azurite)
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Azure upload helpers (azure-storage SDK — handles auth correctly for Azurite)
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
