package com.litebfx.bam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
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

    @Test
    void vfoSplitting_regionQueryDisablesSplitting_singlePartition() {
        // When a region is pushed, we fall back to the single-partition region-query path.
        int numParts = spark.read().format("bam")
                .option("indexPath", baiPath)
                .load(bamPath)
                .filter("referenceName = 'CHROMOSOME_I'")
                .rdd().getNumPartitions();
        assertEquals(1, numParts,
            "region push-down should produce a single partition regardless of BAI");
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
