package com.litebfx.bam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
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
}
