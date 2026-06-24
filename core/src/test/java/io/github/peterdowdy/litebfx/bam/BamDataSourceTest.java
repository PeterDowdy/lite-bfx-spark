package io.github.peterdowdy.litebfx.bam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link BamDataSource} via {@code spark.read.format("bam")}.
 *
 * <p>Requires a local {@link SparkSession}.  Uses {@code range.bam} + {@code range.bam.bai}
 * from test resources (112-record C. elegans BAM, ground truth from samtools 1.21).
 */
class BamDataSourceTest {

    static SparkSession spark;
    static String bamPath;
    static String baiPath;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("BamDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        URL bamUrl = BamDataSourceTest.class.getClassLoader().getResource("range.bam");
        assertNotNull(bamUrl, "range.bam not found in test resources");
        bamPath = java.nio.file.Paths.get(bamUrl.toURI()).toUri().toString();

        URL baiUrl = BamDataSourceTest.class.getClassLoader().getResource("range.bam.bai");
        assertNotNull(baiUrl, "range.bam.bai not found in test resources");
        baiPath = java.nio.file.Paths.get(baiUrl.toURI()).toUri().toString();
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
        StructType schema = spark.read().format("bam").load(bamPath).schema();
        assertEquals(BamSchema.SCHEMA, schema);
    }

    @Test
    void columnNamesSam_reportsSamSchemaAndReadsRows() {
        Dataset<Row> df = spark.read().format("bam")
                .option("columnNames", "sam")
                .load(bamPath);
        assertEquals(BamSchema.SAM_SCHEMA, df.schema());
        // The SAM-named columns are queryable and the data is unchanged.
        assertEquals(112L, df.count());
        // Pick a known mapped read so rname/pos are populated.
        Row r = df.select("qname", "rname", "pos")
                .filter("rname = 'CHROMOSOME_I'")
                .orderBy("pos")
                .first();
        assertNotNull(r.getString(0));
        assertEquals("CHROMOSOME_I", r.getString(1));
        assertTrue(r.getLong(2) > 0);
    }

    @Test
    void columnNamesSam_regionFilterOnRnamePos_pushesDownAndCounts() {
        // Region pushdown must recognize the SAM-spec column names.
        long count = spark.read().format("bam")
                .option("columnNames", "sam")
                .load(bamPath)
                .filter("rname = 'CHROMOSOME_I' AND pos >= 1000 AND pos <= 2000")
                .count();
        assertEquals(12L, count);

        String plan = spark.read().format("bam")
                .option("columnNames", "sam")
                .load(bamPath)
                .filter("rname = 'CHROMOSOME_I'")
                .queryExecution().executedPlan().toString();
        assertFalse(plan.contains("CHROMOSOME_I"),
                "rname equality should be pushed (absent from physical plan) in SAM mode:\n" + plan);
    }

    // -------------------------------------------------------------------------
    // _metadata column (Databricks/Spark file-source compatible)
    // -------------------------------------------------------------------------

    @Test
    void metadata_notInDefaultSchema() {
        StructType schema = spark.read().format("bam").load(bamPath).schema();
        assertFalse(java.util.Arrays.asList(schema.fieldNames()).contains("_metadata"),
                "_metadata is a hidden metadata column and must not appear in the default schema");
    }

    @Test
    void metadata_filePathNameSizeSelectable() {
        Row r = spark.read().format("bam").load(bamPath)
                .selectExpr("_metadata.file_path AS p", "_metadata.file_name AS n",
                            "_metadata.file_size AS s")
                .first();
        assertTrue(r.getString(0).endsWith("range.bam"), "file_path should end with the file name: " + r.getString(0));
        assertEquals("range.bam", r.getString(1));
        assertTrue(r.getLong(2) > 0, "file_size should be positive");
    }

    @Test
    void metadata_wholeStructSelectable_andDataUnchanged() {
        Dataset<Row> df = spark.read().format("bam").load(bamPath).select("_metadata");
        assertEquals(1, df.schema().length());
        assertEquals("_metadata", df.schema().apply(0).name());
        StructType meta = (StructType) df.schema().apply(0).dataType();
        assertEquals("file_path", meta.apply(0).name());
        assertEquals("file_modification_time", meta.apply(3).name());
        assertEquals("index_path", meta.apply(4).name());
        // Selecting only the metadata column does not change the row count.
        assertEquals(112L, df.count());
    }

    @Test
    void metadata_coexistsWithDataColumns() {
        Row r = spark.read().format("bam").load(bamPath)
                .selectExpr("readName", "_metadata.file_name AS n")
                .first();
        assertNotNull(r.getString(0));
        assertEquals("range.bam", r.getString(1));
    }

    @Test
    void metadata_indexPath_populatedWhenBaiUsed() {
        // With a co-located .bai, reads are planned via the index, so index_path is set.
        Row r = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .selectExpr("_metadata.index_path AS idx")
                .first();
        assertNotNull(r.getString(0), "index_path should be set when the BAI index is used");
        assertTrue(r.getString(0).endsWith(".bai"), "index_path: " + r.getString(0));
    }

    @Test
    void metadata_indexPath_nullWhenUseIndexFalse() {
        Row r = spark.read().format("bam")
                .option("useIndex", "false")
                .load(bamPath)
                .selectExpr("_metadata.index_path AS idx")
                .first();
        assertTrue(r.isNullAt(0), "index_path must be null when useIndex=false");
    }

    // -------------------------------------------------------------------------
    // Full-file count
    // -------------------------------------------------------------------------

    @Test
    void count_matchesSamtools() {
        long count = spark.read().format("bam").load(bamPath).count();
        assertEquals(112L, count);
    }

    // -------------------------------------------------------------------------
    // Predicate pushdown (filter applied post-scan for correctness; BAI-based
    // optimization reduces bytes read when index is present)
    // -------------------------------------------------------------------------

    @Test
    void filter_referenceNameEquality_returnsCorrectCount() {
        long count = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .count();
        assertEquals(18L, count);
    }

    @Test
    void filter_referenceNameAndStartRange_returnsCorrectCount() {
        long count = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I' AND start >= 1000 AND start <= 2000")
                .count();
        // Verified: samtools view range.bam CHROMOSOME_I | awk '$4 >= 1000 && $4 <= 2000' | wc -l → 12
        assertEquals(12L, count);
    }

    // -------------------------------------------------------------------------
    // Physical plan (EXPLAIN) — pushed vs. post-scan predicate visibility
    // -------------------------------------------------------------------------

    @Test
    void explain_referenceNameEquality_removedFromPhysicalPlan() {
        // When referenceName equality is in pushedPredicates(), Spark trusts the scan
        // for that condition and removes it from the physical plan (no post-scan Filter).
        // In Spark 4.x this means the literal "CHROMOSOME_I" does not appear in the
        // executedPlan string at all — the only remaining Filter is isnotnull(referenceName).
        String plan = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .queryExecution().executedPlan().toString();
        assertFalse(plan.contains("CHROMOSOME_I"),
                "Pushed equality should be absent from the physical plan (Spark trusts the scan):\n" + plan);
        assertTrue(plan.contains("BatchScan"),
                "Plan should still contain the BatchScan node:\n" + plan);
    }

    @Test
    void explain_startRange_appearsAsPostScanFilter() {
        // start >= 1000 is returned unhandled from pushPredicates() so Spark adds a Filter
        // node above the scan.  The referenceName equality remains pushed (still absent).
        String planEquality = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .queryExecution().executedPlan().toString();
        String planWithRange = spark.read().format("bam").load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I' AND start >= 1000")
                .queryExecution().executedPlan().toString();
        // Pushed equality absent in both plans
        assertFalse(planWithRange.contains("CHROMOSOME_I"),
                "Pushed equality should remain absent when range is also present:\n" + planWithRange);
        // Unhandled start range visible only in the range plan
        assertFalse(planEquality.contains("1000"),
                "Equality-only plan should not mention 1000:\n" + planEquality);
        assertTrue(planWithRange.contains("1000"),
                "Unpushed start >= 1000 should appear as a post-scan Filter:\n" + planWithRange);
    }

    // -------------------------------------------------------------------------
    // No-index fallback / BGZF split
    // -------------------------------------------------------------------------

    @Test
    void noIndexFallback_useIndexFalse_returnsAllRecords() {
        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .load(bamPath)
                .count();
        assertEquals(112L, count);
    }

    @Test
    void bgzfSplit_smallSplitSize_correctTotalCount() throws java.io.IOException {
        // Use the synthetic 10-record BAM (small file) with a 1-byte split size to force
        // many partitions and exercise the full BGZF block-scanning path.
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(tempDir);
        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .option("bgzfSplitSize", "1")
                .load(fx.bam().toUri().toString())
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    @Test
    void bgzfSplit_largeRead_correctTotalCount() throws java.io.IOException {
        // A single 80 000 bp read spans multiple BGZF blocks.
        // Splitting at 1 byte forces many partitions; only one should yield the record.
        java.nio.file.Path largeBam = TestBamGenerator.generateLargeReadBam(tempDir, 80_000);
        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .option("bgzfSplitSize", "1")
                .load(largeBam.toUri().toString())
                .count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Explicit indexPath option
    // -------------------------------------------------------------------------

    @Test
    void explicitIndexPath_option_works() {
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .count();
        assertEquals(18L, count);
    }

    @Test
    void indexPath_nonexistent_fallsBackToColocated() {
        // resolveIndexPath: explicit indexPath points to a file that doesn't exist →
        // falls through to co-located check → finds range.bam.bai next to range.bam.
        // The co-located BAI enables VFO splitting, so the count must still be correct.
        long count = spark.read().format("bam")
                .option("indexPath", "/nonexistent/path/no.bai")
                .load(bamPath)
                .count();
        assertEquals(112L, count);
    }

    @Test
    void indexDir_findsIndex_enablesIndexedRead() throws Exception {
        // resolveIndexPath: indexDir contains <bamName>.bai → candidate exists → returned.
        // Copy the BAI into a temp directory and pass it as indexDir.
        Path baiFile = java.nio.file.Paths.get(new java.net.URI(baiPath));
        Path indexDir = tempDir.resolve("indices");
        java.nio.file.Files.createDirectories(indexDir);
        // BAM filename is "range.bam"; BAI candidate is "range.bam.bai"
        java.nio.file.Files.copy(baiFile, indexDir.resolve("range.bam.bai"));

        long count = spark.read().format("bam")
                .option("indexDir", indexDir.toUri().toString())
                .load(bamPath)
                .count();
        assertEquals(112L, count);
    }

    @Test
    void indexDir_notFound_fallsBackToColocated() {
        // resolveIndexPath: indexDir is an empty directory → candidate missing →
        // falls through to co-located check → finds range.bam.bai → still works.
        Path emptyDir = tempDir.resolve("empty_index_dir");
        emptyDir.toFile().mkdirs();

        long count = spark.read().format("bam")
                .option("indexDir", emptyDir.toUri().toString())
                .load(bamPath)
                .count();
        assertEquals(112L, count);
    }

    // -------------------------------------------------------------------------
    // SAM support
    // -------------------------------------------------------------------------

    @Test
    void sam_read_correctSchemaAndCount() throws IOException {
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(tempDir);
        String samUri = fx.sam().toUri().toString();

        Dataset<Row> df = spark.read().format("bam").load(samUri);
        assertEquals(BamSchema.SCHEMA, df.schema());
        assertEquals(TestBamGenerator.RECORD_COUNT, df.count());
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_selectSubset_returnsReducedSchema() {
        Dataset<Row> df = spark.read().format("bam").load(bamPath)
                .select("readName", "start");
        assertEquals(2, df.schema().length());
        assertEquals("readName", df.schema().apply(0).name());
        assertEquals("start", df.schema().apply(1).name());
    }

    @Test
    void columnPruning_withoutAttributes_doesNotBuildAttributesMap() {
        // Verify that selecting columns excluding "attributes" succeeds
        // and returns the expected count (no NPE from skipped map construction).
        long count = spark.read().format("bam").load(bamPath)
                .select("readName", "flags", "referenceName", "start")
                .count();
        assertEquals(112L, count);
    }

    // -------------------------------------------------------------------------
    // VFO-based per-reference splitting
    // -------------------------------------------------------------------------

    @Test
    void vfoSplitting_countMatchesFullScan() {
        // VFO splitting is active when BAI is present and no region is pushed.
        // range.bam has 7 references in its header → 7 per-ref partitions + 1 unmapped.
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .count();
        assertEquals(112L, count, "VFO-split count must equal full-scan count");
    }

    @Test
    void vfoSplitting_perChromosomeCountsCorrect() {
        Dataset<Row> df = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath);
        assertEquals(18L, df.filter("referenceName = 'CHROMOSOME_I'").count());
        assertEquals(34L, df.filter("referenceName = 'CHROMOSOME_II'").count());
        assertEquals(41L, df.filter("referenceName = 'CHROMOSOME_III'").count());
        assertEquals(19L, df.filter("referenceName = 'CHROMOSOME_IV'").count());
    }

    @Test
    void vfoSplitting_createsMoreThanOnePartition() {
        // range.bam has 7 refs → at least 2 partitions (7 per-ref + 1 unmapped = 8).
        int numParts = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .rdd().getNumPartitions();
        assertTrue(numParts > 1, "VFO splitting should produce multiple partitions; got " + numParts);
    }

    @Test
    void vfoSplitting_numPartitionsCap_reducesPartitionCount() {
        // Cap at 2: 7 refs get grouped into 2 per-ref partitions + 1 unmapped = 3 total.
        int numParts = spark.read().format("bam")
                .option("indexPath", baiPath)
                .option("numPartitions", "2")
                .load(bamPath)
                .rdd().getNumPartitions();
        // 2 ref-group partitions + 1 unmapped partition = 3
        assertEquals(3, numParts,
            "numPartitions=2 should cap ref partitions to 2, plus 1 unmapped");
    }

    @Test
    void vfoSplitting_numPartitionsCap_countStillCorrect() {
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .option("numPartitions", "2")
                .load(bamPath)
                .count();
        assertEquals(112L, count, "count must be correct regardless of numPartitions");
    }

    /**
     * Asserts that a hybrid-split read returns every record exactly once: the row count equals
     * {@code expected} (no records dropped) and the number of distinct read names also equals
     * {@code expected} (no records duplicated). The generated fixtures use unique read names
     * ({@code read0..read(N-1)}), so distinct-name count is an exact de-dup check.
     */
    private static void assertNoDropNoDuplicate(Dataset<Row> df, long expected) {
        assertEquals(expected, df.count(), "record count must be exact (no records dropped)");
        assertEquals(expected, df.select("readName").distinct().count(),
                "distinct read names must equal record count (no records duplicated)");
    }

    @Test
    void hybrid_regionQuery_defaultSplitSize_singlePartitionForTinyFile() {
        // At the 128 MB default, tiny range.bam's CHROMOSOME_I fits in one partition.
        int numParts = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .rdd().getNumPartitions();
        assertEquals(1, numParts, "tiny region fits a single partition at the default split size");
    }

    @Test
    void hybrid_noFilter_multiBlockReference_splitsWithNoRecordsDropped() throws Exception {
        // Realistic case: a reference whose data spans many BGZF blocks (records straddle block
        // boundaries). With a small indexedSplitSize it must divide into many partitions yet return
        // every record exactly once — this is the case byte-offset splitting silently dropped.
        java.nio.file.Path manyBam = TestBamGenerator.generateManyReadsBam(tempDir, 10000);
        Dataset<Row> df = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .load(manyBam.toUri().toString());
        assertTrue(df.rdd().getNumPartitions() > 2,
                "multi-block reference must over-split; got " + df.rdd().getNumPartitions());
        assertNoDropNoDuplicate(df, 10000L);
    }

    @Test
    void hybrid_regionQuery_multiBlockReference_splitsWithNoRecordsDropped() throws Exception {
        // Same fixture, but with a pushed referenceName filter (region path). The split must be
        // exact: no dropped and no duplicated records across partitions.
        java.nio.file.Path manyBam = TestBamGenerator.generateManyReadsBam(tempDir, 10000);
        Dataset<Row> df = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .load(manyBam.toUri().toString())
                .filter("referenceName = '" + TestBamGenerator.REF_NAME + "'");
        assertTrue(df.rdd().getNumPartitions() > 1,
                "a large pushed region must split into multiple partitions; got " + df.rdd().getNumPartitions());
        assertNoDropNoDuplicate(df, 10000L);
    }

    @Test
    void hybrid_regionQuery_withStartRange_splits_countExact() throws Exception {
        // A coordinate sub-range on a large reference: the count matches an unsplit reference scan
        // filtered to the same range (Spark post-filters the coordinate range; the reader guarantees
        // the reference and the exact union).
        java.nio.file.Path manyBam = TestBamGenerator.generateManyReadsBam(tempDir, 10000);
        String rangePred = "referenceName = '" + TestBamGenerator.REF_NAME
                + "' AND start >= 400000 AND start <= 600000";
        long expected = spark.read().format("bam")
                .option("indexedSplitSize", "134217728")  // unsplit reference scan as ground truth
                .load(manyBam.toUri().toString())
                .filter(rangePred).count();
        assertTrue(expected > 0, "fixture should have reads in the queried range");
        Dataset<Row> ranged = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .load(manyBam.toUri().toString())
                .filter(rangePred);
        assertNoDropNoDuplicate(ranged, expected);
    }

    @Test
    void hybrid_perChromosomeCounts_exactUnderSplitting() {
        // Per-reference correctness for a real multi-reference BAM under the region path.
        Dataset<Row> df = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath);
        assertEquals(18L, df.filter("referenceName = 'CHROMOSOME_I'").count());
        assertEquals(34L, df.filter("referenceName = 'CHROMOSOME_II'").count());
        assertEquals(41L, df.filter("referenceName = 'CHROMOSOME_III'").count());
        assertEquals(19L, df.filter("referenceName = 'CHROMOSOME_IV'").count());
    }

    @Test
    void unindexed_multiBlockReference_splitsWithNoRecordsDropped() throws Exception {
        // Unindexed (useIndex=false) BGZF byte-range split over a reference spanning many BGZF
        // blocks where records straddle block boundaries. The split guesser must locate the first
        // record start *within* a block so every partition orients — no records dropped or duplicated.
        java.nio.file.Path manyBam = TestBamGenerator.generateManyReadsBam(tempDir, 10000);
        Dataset<Row> df = spark.read().format("bam")
                .option("useIndex", "false")
                .option("bgzfSplitSize", "4096")
                .load(manyBam.toUri().toString());
        assertTrue(df.rdd().getNumPartitions() > 2,
                "small bgzfSplitSize must produce many partitions; got " + df.rdd().getNumPartitions());
        assertNoDropNoDuplicate(df, 10000L);
    }

    @Test
    void hybrid_noFilter_wholeFileCount_exactWithDefaultAndSmallSplit() throws Exception {
        // Whole-file count is identical whether the reference is read as one partition (default) or
        // split into many (small indexedSplitSize) — the split changes parallelism, not results.
        java.nio.file.Path manyBam = TestBamGenerator.generateManyReadsBam(tempDir, 10000);
        long deflt = spark.read().format("bam").load(manyBam.toUri().toString()).count();
        long split = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .load(manyBam.toUri().toString()).count();
        assertEquals(10000L, deflt);
        assertEquals(deflt, split, "splitting must not change the record count");
    }

    // -------------------------------------------------------------------------
    // Skew-aware fallback: more references than numPartitions
    // -------------------------------------------------------------------------

    // 12 references (heavy chrBig + 11 small) but only numPartitions=4, with a small
    // indexedSplitSize so the heavy reference's data exceeds one split. This is the path
    // (refs > numPartitions) that previously fell back to equal-count reference grouping,
    // bundling the heavy reference into one straggler partition.
    private static final int SKEW_SMALL_REFS = 11;
    private static final int SKEW_HEAVY_READS = 6000;
    private static final int SKEW_SMALL_READS = 5;
    private static final long SKEW_TOTAL = SKEW_HEAVY_READS + (long) SKEW_SMALL_REFS * SKEW_SMALL_READS;

    @Test
    void skewAwareFallback_heavyReferenceSubSplitAcrossPartitions() throws Exception {
        // With refs (12) > numPartitions (4), the heavy reference must STILL be byte-split rather
        // than packed whole into a single group — so its reads span multiple input partitions.
        // spark_partition_id() (no shuffle in this plan) reports the originating input partition.
        java.nio.file.Path skewBam = TestBamGenerator.generateSkewedMultiRefBam(
                tempDir, SKEW_SMALL_REFS, SKEW_HEAVY_READS, SKEW_SMALL_READS);
        Dataset<Row> df = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .option("numPartitions", "4")
                .load(skewBam.toUri().toString());

        long heavyPartitions = df.filter("referenceName = 'chrBig'")
                .select(functions.spark_partition_id().alias("pid"))
                .distinct().count();
        assertTrue(heavyPartitions >= 2,
                "heavy reference must be sub-split across partitions even when refs > numPartitions; got "
                        + heavyPartitions);
    }

    @Test
    void skewAwareFallback_allRecordsPreservedNoDropNoDuplicate() throws Exception {
        java.nio.file.Path skewBam = TestBamGenerator.generateSkewedMultiRefBam(
                tempDir, SKEW_SMALL_REFS, SKEW_HEAVY_READS, SKEW_SMALL_READS);
        Dataset<Row> df = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .option("numPartitions", "4")
                .load(skewBam.toUri().toString());
        assertNoDropNoDuplicate(df, SKEW_TOTAL);
    }

    @Test
    void skewAwareFallback_perReferenceCountsExact() throws Exception {
        // Every reference's reads survive the byte-balanced grouping — the heavy one and all 11
        // small ones — at their exact counts.
        java.nio.file.Path skewBam = TestBamGenerator.generateSkewedMultiRefBam(
                tempDir, SKEW_SMALL_REFS, SKEW_HEAVY_READS, SKEW_SMALL_READS);
        Dataset<Row> df = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .option("numPartitions", "4")
                .load(skewBam.toUri().toString());
        assertEquals(SKEW_HEAVY_READS, df.filter("referenceName = 'chrBig'").count());
        for (int s = 1; s <= SKEW_SMALL_REFS; s++) {
            String ref = String.format("chr%02d", s);
            assertEquals(SKEW_SMALL_READS, df.filter("referenceName = '" + ref + "'").count(),
                    "small reference " + ref + " must keep all its reads");
        }
    }

    @Test
    void skewAwareFallback_wholeFileCountMatchesUnsplit() throws Exception {
        // The skew-aware fallback changes parallelism, not results: the count equals a single-
        // partition ground-truth read (large numPartitions, default split = one partition per ref).
        java.nio.file.Path skewBam = TestBamGenerator.generateSkewedMultiRefBam(
                tempDir, SKEW_SMALL_REFS, SKEW_HEAVY_READS, SKEW_SMALL_READS);
        long groundTruth = spark.read().format("bam")
                .option("numPartitions", "500")
                .load(skewBam.toUri().toString()).count();
        long fallback = spark.read().format("bam")
                .option("indexedSplitSize", "4096")
                .option("numPartitions", "4")
                .load(skewBam.toUri().toString()).count();
        assertEquals(SKEW_TOTAL, groundTruth);
        assertEquals(groundTruth, fallback, "skew-aware grouping must not change the record count");
    }

    // -------------------------------------------------------------------------
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_countMatchesSamtools() {
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW bam_view USING bam"
                + " OPTIONS (path '" + bamPath + "')");
        long count = spark.sql("SELECT count(*) FROM bam_view").first().getLong(0);
        assertEquals(112L, count);
        spark.sql("DROP VIEW IF EXISTS bam_view");
    }

    @Test
    void sql_createTempView_regionFilter_returnsCorrectCount() {
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW bam_region_view USING bam"
                + " OPTIONS (path '" + bamPath + "')");
        long count = spark.sql(
                "SELECT count(*) FROM bam_region_view WHERE referenceName = 'CHROMOSOME_I'")
                .first().getLong(0);
        assertEquals(18L, count);
        spark.sql("DROP VIEW IF EXISTS bam_region_view");
    }

    // -------------------------------------------------------------------------
    // Directory and glob resolution (exercises BamScan.collectBamChildren)
    // -------------------------------------------------------------------------

    @Test
    void directory_ofBamFiles_readsAllRecords() throws Exception {
        // Single directory path → globStatus returns [dirStatus] → collectBamChildren called.
        // Generate in a staging dir; copy only the two BAMs (not the SAM) so the count
        // is RECORD_COUNT * 2, not * 3 (isAcceptedExtension also accepts .sam).
        Path stage = tempDir.resolve("stage_dir");
        Files.createDirectories(stage);
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(stage);
        Path dir = tempDir.resolve("bamdir");
        Files.createDirectories(dir);
        Files.copy(fx.bam(), dir.resolve("a.bam"));
        Files.copy(fx.bam(), dir.resolve("b.bam"));

        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .load(dir.toUri().toString())
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT * 2L, count);
    }

    @Test
    void directory_withNonBamFiles_ignoresNonBam() throws Exception {
        // isAcceptedExtension returns false for .txt — only the BAM file is collected.
        Path stage = tempDir.resolve("stage_mixed");
        Files.createDirectories(stage);
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(stage);
        Path dir = tempDir.resolve("mixeddir");
        Files.createDirectories(dir);
        Files.copy(fx.bam(), dir.resolve("reads.bam"));
        Files.writeString(dir.resolve("notes.txt"), "ignored");

        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .load(dir.toUri().toString())
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    @Test
    void glob_multipleFiles_readsAll() throws Exception {
        // Glob pattern → globStatus returns multiple FileStatus → else-if branch in
        // resolveBamFiles; each file added via isAcceptedExtension (not collectBamChildren).
        Path stage = tempDir.resolve("stage_glob");
        Files.createDirectories(stage);
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(stage);
        Path dir = tempDir.resolve("globdir");
        Files.createDirectories(dir);
        Files.copy(fx.bam(), dir.resolve("a.bam"));
        Files.copy(fx.bam(), dir.resolve("b.bam"));

        String glob = dir.toUri().toString() + "/*.bam";
        long count = spark.read().format("bam")
                .option("useIndex", "false")
                .load(glob)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT * 2L, count);
    }

    // -------------------------------------------------------------------------
    // SAM edge cases
    // -------------------------------------------------------------------------

    @Test
    void sam_headerOnly_returnsEmptyDataset() throws Exception {
        // SAM file with only @HD and @SQ lines — no data records.
        // Exercises openSamSplit() "line == null → dataStartByte = lineEndPos (EOF)" branch.
        Path samPath = tempDir.resolve("headeronly.sam");
        java.nio.file.Files.writeString(samPath,
                "@HD\tVN:1.6\tSO:coordinate\n" +
                "@SQ\tSN:chr1\tLN:1000000\n",
                java.nio.charset.StandardCharsets.UTF_8);

        long count = spark.read().format("bam")
                .load(samPath.toUri().toString())
                .count();
        assertEquals(0L, count);
    }

    // -------------------------------------------------------------------------
    // Error conditions
    // -------------------------------------------------------------------------

    @Test
    void missingFile_throwsException() {
        assertThrows(Exception.class, () ->
            spark.read().format("bam").load("/no/such/file.bam").count());
    }

    @Test
    void emptyFilter_returnsZeroRows() {
        // A region that contains no reads should return 0, not throw.
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .filter("referenceName = 'NONEXISTENT_CHROM'")
                .count();
        assertEquals(0L, count);
    }

    // -------------------------------------------------------------------------
    // Statistics (SupportsReportStatistics)
    // -------------------------------------------------------------------------

    @Test
    void estimateStatistics_sizeInBytes_greaterThanZero() {
        Dataset<Row> df = spark.read().format("bam").load(bamPath);
        long sizeBytes = df.queryExecution().optimizedPlan()
                .stats().sizeInBytes().longValue();
        assertTrue(sizeBytes > 0, "sizeInBytes should be > 0 for a non-empty BAM file");
        assertTrue(sizeBytes <= new java.io.File(java.nio.file.Paths.get(
                java.net.URI.create(bamPath)).toString()).length() * 2,
                "sizeInBytes should be within 2x of the actual file size");
    }

    // -------------------------------------------------------------------------
    // Limit pushdown (SupportsPushDownLimit)
    // -------------------------------------------------------------------------

    @Test
    void limit_pushdown_returnsExactCount() {
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .limit(3)
                .count();
        assertEquals(3L, count, "limit(3) should return exactly 3 rows");
    }

    @Test
    void limit_withRegionFilter_returnsCorrectRows() {
        // Region filter combined with limit: region still applied first, limit on top.
        long count = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .limit(2)
                .count();
        assertEquals(2L, count, "region filter + limit(2) should return exactly 2 rows");
    }

    // -------------------------------------------------------------------------
    // Ordering (SupportsReportOrdering)
    // -------------------------------------------------------------------------

    @Test
    void outputOrdering_coordinateSortedWithIndex_returnsReferenceNameAndStart() {
        // range.bam is coordinate-sorted and has a co-located .bai
        BamScan scan = new BamScan(
                new CaseInsensitiveStringMap(java.util.Map.of("path", bamPath)),
                BamSchema.SCHEMA, false, null, 1, Integer.MAX_VALUE, false);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(2, ordering.length, "coordinate-sorted BAM with index should report 2 ordering fields");
        assertEquals("referenceName", ((NamedReference) ordering[0].expression()).fieldNames()[0]);
        assertEquals("start",         ((NamedReference) ordering[1].expression()).fieldNames()[0]);
    }

    @Test
    void outputOrdering_sam_isEmpty() throws Exception {
        TestBamGenerator.Fixtures fx = TestBamGenerator.generate(tempDir);
        BamScan scan = new BamScan(
                new CaseInsensitiveStringMap(java.util.Map.of("path", fx.sam().toUri().toString())),
                BamSchema.SCHEMA, false, null, 1, Integer.MAX_VALUE, false);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(0, ordering.length, "SAM without index should report no ordering");
    }

    @Test
    void outputOrdering_querynameSortedBam_isEmpty() throws Exception {
        java.nio.file.Path qnameBam = TestBamGenerator.generateQuerynameSortedBam(tempDir);
        BamScan scan = new BamScan(
                new CaseInsensitiveStringMap(java.util.Map.of("path", qnameBam.toUri().toString())),
                BamSchema.SCHEMA, false, null, 1, Integer.MAX_VALUE, false);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(0, ordering.length, "queryname-sorted BAM should report no ordering");
    }
}
