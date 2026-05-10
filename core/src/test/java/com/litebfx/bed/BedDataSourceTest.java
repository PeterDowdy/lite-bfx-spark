package com.litebfx.bed;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.tribble.bed.BEDCodec;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

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
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_countMatchesExpected() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW bed_view USING bed"
                + " OPTIONS (path '" + bedPath + "')");
        long count = spark.sql("SELECT count(*) FROM bed_view").first().getLong(0);
        assertEquals(BedTestGenerator.BED6_TOTAL, count);
        spark.sql("DROP VIEW IF EXISTS bed_view");
    }

    @Test
    void sql_createTempView_regionFilter_returnsCorrectCount() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath = fx.bed6Bgzf().toString();
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW bed_region_view USING bed"
                + " OPTIONS (path '" + bedPath + "')");
        long count = spark.sql(
                "SELECT count(*) FROM bed_region_view WHERE chrom = 'chr1'")
                .first().getLong(0);
        assertEquals(BedTestGenerator.BED6_CHR1_COUNT, count);
        spark.sql("DROP VIEW IF EXISTS bed_region_view");
    }

    // -------------------------------------------------------------------------
    // .bgz extension — looksCompressed() covers .bgz branch in planInputPartitions
    // -------------------------------------------------------------------------

    @Test
    void bgzExtension_readsAllRecords() throws Exception {
        // Write a BGZF file with .bgz extension — covers the ".bgz" branch in
        // planInputPartitions()'s isCompressed check.
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        Path src = Paths.get(fx.bed6Bgzf());
        Path bgzPath = tempDir.resolve("test.bed.bgz");
        Files.copy(src, bgzPath, StandardCopyOption.REPLACE_EXISTING);

        long count = spark.read().format("bed").load(bgzPath.toUri().toString()).count();
        assertEquals(BedTestGenerator.BED6_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // Explicit indexPath option — resolveIndexPath() explicit branch
    // -------------------------------------------------------------------------

    @Test
    void explicitIndexPath_existingFile_usedForRegionQuery() throws Exception {
        // Pass indexPath explicitly → resolveIndexPath() takes the explicit path branch.
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath   = fx.bed6Bgzf().toString();
        String indexPath = fx.bed6Tbi().toString();

        long count = spark.read().format("bed")
                .option("indexPath", indexPath)
                .load(bedPath)
                .filter("chrom = 'chr1'")
                .count();
        assertEquals(BedTestGenerator.BED6_CHR1_COUNT, count);
    }

    @Test
    void explicitIndexPath_nonExistentFile_fallsBackToColocatedIndex() throws Exception {
        // Explicit indexPath that does not exist → resolveIndexPath() falls through
        // to co-located .tbi check, which succeeds.
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bedPath      = fx.bed6Bgzf().toString();
        String badIndexPath = tempDir.resolve("does_not_exist.tbi").toUri().toString();

        long count = spark.read().format("bed")
                .option("indexPath", badIndexPath)
                .load(bedPath)
                .count();
        // Falls back to co-located .tbi → returns all records
        assertEquals(BedTestGenerator.BED6_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // planBedSplitPartitions — multiple chunks (bedSplitSize smaller than file)
    // -------------------------------------------------------------------------

    @Test
    void plainBed_smallSplitSize_multiplePartitions_countCorrect() throws Exception {
        // Use a tiny bedSplitSize to force multiple chunks, covering the
        // i != numChunks-1 branch (endByte != Long.MAX_VALUE) in planBedSplitPartitions.
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String bed3Path = fx.bed3Plain().toString();

        // 10 bytes per split ensures the 2-record BED3 file splits into >= 2 chunks
        long count = spark.read().format("bed")
                .option("bedSplitSize", "10")
                .load(bed3Path)
                .count();
        assertEquals(BedTestGenerator.BED3_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // resolveIndexPath — co-located .csi branch (no .tbi, only .csi)
    // -------------------------------------------------------------------------

    @Test
    void csiIndex_colocated_usedForRegionQuery() throws Exception {
        // Write a BGZF bed file in a fresh directory that has no .tbi.
        // Write the tabix index bytes as <bed>.csi — resolveIndexPath() falls through
        // the .tbi check and picks up the .csi file.  TabixIndex reads tabix-format
        // bytes regardless of file extension.
        Path csiDir = tempDir.resolve("csi_test");
        Files.createDirectory(csiDir);
        Path bedPath = csiDir.resolve("query.bed.gz");
        Path csiPath = csiDir.resolve("query.bed.gz.csi");

        try (BlockCompressedOutputStream out =
                     new BlockCompressedOutputStream(bedPath.toFile())) {
            PrintWriter pw = new PrintWriter(out);
            pw.println("chr1\t100\t200\tpeak1\t500\t+");
            pw.println("chr2\t100\t300\tpeak2\t700\t+");
            pw.flush();
        }
        // Write tabix data to a .csi file (the resolver checks fs.exists(), not content)
        TabixIndex tbi = IndexFactory.createTabixIndex(bedPath.toFile(), new BEDCodec(), null);
        tbi.write(csiPath);

        // Verify: no .tbi exists, so resolveIndexPath() must reach the .csi branch
        assertFalse(Files.exists(csiDir.resolve("query.bed.gz.tbi")), ".tbi must not exist");
        assertTrue(Files.exists(csiPath), ".csi must exist");

        long chr1Count = spark.read().format("bed")
                .load(bedPath.toUri().toString())
                .filter("chrom = 'chr1'")
                .count();
        assertEquals(1L, chr1Count);
    }

    // -------------------------------------------------------------------------
    // planBedSplitPartitions — splitSize <= 0 throws IllegalArgumentException
    // -------------------------------------------------------------------------

    @Test
    void planBedSplitPartitions_negativeSplitSize_throwsException() throws Exception {
        // bedSplitSize=-1 triggers the splitSize <= 0 guard in planBedSplitPartitions().
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        assertThrows(Exception.class, () ->
                spark.read().format("bed")
                        .option("bedSplitSize", "-1")
                        .load(fx.bed3Plain().toString())
                        .count());
    }

    // -------------------------------------------------------------------------
    // planInputPartitions — pathStr == null throws IllegalArgumentException
    // -------------------------------------------------------------------------

    @Test
    void bedScan_noPathOption_throwsIllegalArgument() {
        // Instantiate BedScan directly with an empty options map (no "path" key).
        // planInputPartitions() throws before reaching the SparkSession call.
        org.apache.spark.sql.util.CaseInsensitiveStringMap empty =
                new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Map.of());
        BedScan scan = new BedScan(empty, BedSchema.SCHEMA, null, 0L, Long.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, scan::planInputPartitions);
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
