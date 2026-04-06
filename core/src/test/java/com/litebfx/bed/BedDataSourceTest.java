package com.litebfx.bed;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link BedDataSource} via {@code spark.read.format("bed")}.
 *
 * <p>Tests cover schema validation, full-file counts, tabix region queries,
 * BED3 null fields, BED12 block fields, and column pruning.
 */
class BedDataSourceTest {

    static SparkSession spark;
    static String exampleBedGzPath;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("BedDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        URL url = BedDataSourceTest.class.getClassLoader().getResource("example.bed.gz");
        assertNotNull(url, "example.bed.gz not found in test resources");
        exampleBedGzPath = java.nio.file.Paths.get(url.toURI()).toUri().toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    @Test
    void schema_matchesBedSchema() {
        Dataset<Row> df = spark.read().format("bed").load(exampleBedGzPath);
        assertEquals(BedSchema.SCHEMA, df.schema());
    }

    // -------------------------------------------------------------------------
    // Full-file count (example.bed.gz — 3432 lines, no tabix index)
    // -------------------------------------------------------------------------

    @Test
    void count_exampleBedGz_matchesLineCount() {
        long count = spark.read().format("bed").load(exampleBedGzPath).count();
        assertEquals(3432L, count);
    }

    // -------------------------------------------------------------------------
    // Tabix region query
    // -------------------------------------------------------------------------

    @Test
    void tabixRegionQuery_returnsCorrectCount() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();

        // chr1 only
        long chr1Count = spark.read().format("bed").load(bedPath)
                .filter("chrom = 'chr1'")
                .count();
        assertEquals(BedTestGenerator.BED6_CHR1_COUNT, chr1Count);

        // chr1, chromStart >= 500
        long chr1From500 = spark.read().format("bed").load(bedPath)
                .filter("chrom = 'chr1' AND chromStart >= 500")
                .count();
        assertEquals(BedTestGenerator.BED6_CHR1_FROM_500, chr1From500);
    }

    @Test
    void tabixRegionQuery_totalCount_matchesAll() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();

        long total = spark.read().format("bed").load(bedPath).count();
        assertEquals(BedTestGenerator.BED6_TOTAL, total);
    }

    // -------------------------------------------------------------------------
    // No-index fallback
    // -------------------------------------------------------------------------

    @Test
    void noIndexFallback_useIndexFalse_returnsAllRecords() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();

        long count = spark.read().format("bed")
                .option("useIndex", "false")
                .load(bedPath)
                .count();
        assertEquals(BedTestGenerator.BED6_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // BED3 file — fields beyond chromEnd are null
    // -------------------------------------------------------------------------

    @Test
    void bed3_extraFieldsAreNull() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bed3Path = fx.bed3Plain().toString();

        Dataset<Row> df = spark.read().format("bed").load(bed3Path);
        assertEquals(BedTestGenerator.BED3_TOTAL, df.count());

        Row first = df.orderBy("chromStart").first();
        assertEquals("chr1", first.getString(0));   // chrom
        assertEquals(100L,   first.getLong(1));      // chromStart (0-based)
        assertEquals(200L,   first.getLong(2));      // chromEnd
        assertTrue(first.isNullAt(3),  "name should be null for BED3");
        assertTrue(first.isNullAt(4),  "score should be null for BED3");
        assertTrue(first.isNullAt(5),  "strand should be null for BED3");
        assertTrue(first.isNullAt(6),  "thickStart should be null");
        assertTrue(first.isNullAt(7),  "thickEnd should be null");
        assertTrue(first.isNullAt(8),  "itemRgb should be null for BED3");
        assertTrue(first.isNullAt(9),  "blockCount should be null for BED3");
        assertTrue(first.isNullAt(10), "blockSizes should be null for BED3");
        assertTrue(first.isNullAt(11), "blockStarts should be null for BED3");
    }

    // -------------------------------------------------------------------------
    // BED12 file — block fields populated
    // -------------------------------------------------------------------------

    @Test
    void bed12_blockFieldsPopulated() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bed12Path = fx.bed12Bgzf().toString();

        Dataset<Row> df = spark.read().format("bed").load(bed12Path);
        assertEquals(BedTestGenerator.BED12_TOTAL, df.count());

        Row row = df.first();
        assertEquals("chr1", row.getString(0));  // chrom
        assertEquals(0L,     row.getLong(1));     // chromStart
        assertEquals(1000L,  row.getLong(2));     // chromEnd

        // blockCount
        assertFalse(row.isNullAt(9), "blockCount should not be null for BED12");
        assertEquals(3, row.getInt(9));

        // blockSizes and blockStarts
        assertFalse(row.isNullAt(10), "blockSizes should not be null for BED12");
        assertFalse(row.isNullAt(11), "blockStarts should not be null for BED12");
    }

    // -------------------------------------------------------------------------
    // BED6 field values
    // -------------------------------------------------------------------------

    @Test
    void bed6_fieldValues_firstRecord() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();

        Row first = spark.read().format("bed").load(bedPath)
                .orderBy("chrom", "chromStart")
                .first();

        assertEquals("chr1",   first.getString(0));  // chrom
        assertEquals(100L,     first.getLong(1));     // chromStart (0-based)
        assertEquals(200L,     first.getLong(2));     // chromEnd
        assertEquals("peak1",  first.getString(3));   // name
        assertEquals(500,      first.getInt(4));      // score
        assertEquals("+",      first.getString(5));   // strand
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_selectSubset_returnsReducedSchema() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();

        Dataset<Row> df = spark.read().format("bed").load(bedPath)
                .select("chrom", "chromStart", "chromEnd");
        assertEquals(3, df.schema().length());
        assertEquals("chrom",      df.schema().apply(0).name());
        assertEquals("chromStart", df.schema().apply(1).name());
        assertEquals("chromEnd",   df.schema().apply(2).name());
        assertEquals(BedTestGenerator.BED6_TOTAL, df.count());
    }
}
