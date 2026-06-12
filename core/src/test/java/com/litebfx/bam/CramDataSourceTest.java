package com.litebfx.bam;

import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.cram.structure.CRAMEncodingStrategy;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link CramDataSource} via {@code spark.read.format("cram")}.
 *
 * <p>Uses a synthetic CRAM file backed by a generated FASTA reference.
 */
public class CramDataSourceTest {

    static SparkSession spark;

    @TempDir
    static Path tempDir;

    static String cramPath;
    static String fastaPath;
    /** CRAM with multiple containers (2 reads/slice, 1 slice/container) for split tests. */
    static String multiCramPath;

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("CramDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        Path faPath = generateFasta(tempDir);
        fastaPath = faPath.toAbsolutePath().toString();
        cramPath = generateCram(tempDir, faPath).toUri().toString();
        multiCramPath = generateMultiContainerCram(tempDir, faPath).toUri().toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    @Test
    void schema_matchesBamSchema() {
        StructType schema = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .schema();
        assertEquals(BamSchema.SCHEMA, schema);
    }

    // -------------------------------------------------------------------------
    // Full-file count
    // -------------------------------------------------------------------------

    @Test
    void count_matchesRecordCount() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // referenceName filter (post-scan; all reads are on chr1)
    // -------------------------------------------------------------------------

    @Test
    void filter_referenceNameEquality_returnsCorrectCount() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .filter("referenceName = 'chr1'")
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_withoutAttributes_succeeds() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .select("readName", "start")
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // No-index fallback
    // -------------------------------------------------------------------------

    @Test
    void noIndexFallback_useIndexFalse_returnsAllRecords() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .option("useIndex", "false")
                .load(cramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Multi-partition (CRAI-based container splitting)
    // -------------------------------------------------------------------------

    @Test
    void multiPartition_withIndex_plansMultiplePartitions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("path", multiCramPath);
        opts.put("referenceFile", fastaPath);
        BamScan scan = new BamScan(new CaseInsensitiveStringMap(opts),
                BamSchema.SCHEMA, true, null, 1, Integer.MAX_VALUE, true);
        InputPartition[] partitions = scan.planInputPartitions();
        assertTrue(partitions.length > 1,
                "Expected >1 partitions from CRAI-based split, got " + partitions.length);
    }

    @Test
    void multiPartition_withIndex_allRecordsRead() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(multiCramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    @Test
    void multiPartition_noIndex_plansMultiplePartitions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("path", multiCramPath);
        opts.put("referenceFile", fastaPath);
        opts.put("useIndex", "false");
        BamScan scan = new BamScan(new CaseInsensitiveStringMap(opts),
                BamSchema.SCHEMA, true, null, 1, Integer.MAX_VALUE, true);
        InputPartition[] partitions = scan.planInputPartitions();
        assertTrue(partitions.length > 1,
                "Expected >1 partitions from container header scan, got " + partitions.length);
    }

    @Test
    void multiPartition_noIndex_allRecordsRead() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .option("useIndex", "false")
                .load(multiCramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_withReferenceFile_countMatchesExpected() {
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW cram_view USING cram"
                + " OPTIONS (path '" + cramPath + "', referenceFile '" + fastaPath + "')");
        long count = spark.sql("SELECT count(*) FROM cram_view").first().getLong(0);
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
        spark.sql("DROP VIEW IF EXISTS cram_view");
    }

    @Test
    void sql_createTempView_regionFilter_returnsCorrectCount() {
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW cram_region_view USING cram"
                + " OPTIONS (path '" + cramPath + "', referenceFile '" + fastaPath + "')");
        long count = spark.sql(
                "SELECT count(*) FROM cram_region_view WHERE referenceName = 'chr1'")
                .first().getLong(0);
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
        spark.sql("DROP VIEW IF EXISTS cram_region_view");
    }

    // -------------------------------------------------------------------------
    // Statistics (SupportsReportStatistics)
    // -------------------------------------------------------------------------

    @Test
    void estimateStatistics_sizeInBytes_greaterThanZero() {
        Dataset<Row> df = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath);
        long sizeBytes = df.queryExecution().optimizedPlan()
                .stats().sizeInBytes().longValue();
        assertTrue(sizeBytes > 0, "sizeInBytes should be > 0 for a non-empty CRAM file");
        assertTrue(sizeBytes <= new java.io.File(java.nio.file.Paths.get(
                java.net.URI.create(cramPath)).toString()).length() * 2,
                "sizeInBytes should be within 2x of the actual file size");
    }

    // -------------------------------------------------------------------------
    // Limit pushdown (SupportsPushDownLimit)
    // -------------------------------------------------------------------------

    @Test
    void limit_pushdown_returnsExactCount() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .limit(3)
                .count();
        assertEquals(3L, count, "limit(3) should return exactly 3 rows from CRAM");
    }

    // -------------------------------------------------------------------------
    // Ordering (SupportsReportOrdering)
    // -------------------------------------------------------------------------

    @Test
    void outputOrdering_craiIndexed_returnsReferenceNameAndStart() {
        // multiCramPath has a co-located .crai and is coordinate-sorted
        BamScan scan = new BamScan(
                new CaseInsensitiveStringMap(Map.of("path", multiCramPath)),
                BamSchema.SCHEMA, false, null, 1, Integer.MAX_VALUE, true);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(2, ordering.length, "CRAM with CRAI should report 2 ordering fields");
        assertEquals("referenceName", ((NamedReference) ordering[0].expression()).fieldNames()[0]);
        assertEquals("start",         ((NamedReference) ordering[1].expression()).fieldNames()[0]);
    }

    @Test
    void outputOrdering_cramWithoutIndex_isEmpty() {
        // cramPath has no .crai
        BamScan scan = new BamScan(
                new CaseInsensitiveStringMap(Map.of("path", cramPath)),
                BamSchema.SCHEMA, false, null, 1, Integer.MAX_VALUE, true);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(0, ordering.length, "CRAM without CRAI should report no ordering");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes a minimal FASTA reference for chr1 (all 'A' bases) and its .fai index.
     */
    public static Path generateFasta(Path dir) throws IOException {
        Path faPath = dir.resolve("ref.fa");
        try (PrintWriter w = new PrintWriter(faPath.toFile())) {
            w.println(">" + TestBamGenerator.REF_NAME);
            String line = "A".repeat(70);
            int remaining = TestBamGenerator.REF_LENGTH;
            while (remaining > 0) {
                int len = Math.min(70, remaining);
                w.println("A".repeat(len));
                remaining -= len;
            }
        }
        FastaSequenceIndexCreator.create(faPath, true);
        return faPath;
    }

    /**
     * Writes a coordinate-sorted CRAM containing the same 10 synthetic records
     * as {@link TestBamGenerator}, backed by the provided FASTA reference.
     */
    public static Path generateCram(Path dir, Path fastaRef) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(TestBamGenerator.REF_NAME, TestBamGenerator.REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        File cramFile = dir.resolve("test.cram").toFile();
        ReferenceSource refSource = new ReferenceSource(fastaRef.toFile());
        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .makeCRAMWriter(header, true, cramFile, fastaRef.toFile())) {
            for (SAMRecord r : buildRecords(header)) {
                writer.addAlignment(r);
            }
        }
        return cramFile.toPath();
    }

    /**
     * Writes a CRAM file backed by {@code fastaRef} using a small encoding strategy
     * (2 reads per slice, 1 slice per container) so that {@value TestBamGenerator#RECORD_COUNT}
     * records are spread across multiple containers.  A co-located {@code .crai} index is
     * also written (as {@code <file>.cram.crai}).
     */
    static Path generateMultiContainerCram(Path dir, Path fastaRef) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(TestBamGenerator.REF_NAME, TestBamGenerator.REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        // setMinimumSingleReferenceSliceSize must be called before setReadsPerSlice
        // because htsjdk validates readsPerSlice >= minimumSingleReferenceSliceSize.
        CRAMEncodingStrategy strategy = new CRAMEncodingStrategy()
                .setMinimumSingleReferenceSliceSize(2)
                .setReadsPerSlice(2)
                .setSlicesPerContainer(1);

        File cramFile = dir.resolve("multi.cram").toFile();
        File craiFile = dir.resolve("multi.cram.crai").toFile();

        ReferenceSource refSource = new ReferenceSource(fastaRef.toFile());
        try (FileOutputStream cramOut = new FileOutputStream(cramFile);
             FileOutputStream craiOut = new FileOutputStream(craiFile)) {
            CRAMCRAIIndexer indexer = new CRAMCRAIIndexer(craiOut, header);
            CRAMContainerStreamWriter writer = new CRAMContainerStreamWriter(
                    strategy, refSource, header, cramOut, indexer, cramFile.getName());
            writer.writeHeader(header);
            for (SAMRecord r : buildRecords(header)) {
                writer.writeAlignment(r);
            }
            writer.finish(true); // also calls indexer.finish() internally
        }
        return cramFile.toPath();
    }

    private static SAMRecord[] buildRecords(SAMFileHeader header) {
        SAMRecord[] records = new SAMRecord[TestBamGenerator.RECORD_COUNT];
        for (int i = 0; i < TestBamGenerator.RECORD_COUNT; i++) {
            SAMRecord r = new SAMRecord(header);
            r.setReadName("read" + (i + 1));
            r.setFlags(0);
            r.setReferenceIndex(0);
            r.setAlignmentStart((i + 1) * 100);
            r.setMappingQuality(TestBamGenerator.MAPPING_QUALITY);
            r.setCigarString(TestBamGenerator.CIGAR);
            r.setReadString(TestBamGenerator.SEQUENCE);
            r.setBaseQualityString(TestBamGenerator.BASE_QUALITIES);
            r.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
            r.setMateAlignmentStart(0);
            r.setInferredInsertSize(0);
            r.setAttribute("NM", 0);
            records[i] = r;
        }
        return records;
    }
}
