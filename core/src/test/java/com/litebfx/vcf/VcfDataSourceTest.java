package com.litebfx.vcf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link VcfDataSource} via {@code spark.read.format("vcf")}.
 *
 * <p>Tests cover schema validation, full-file counts, tabix region queries,
 * INFO/genotype field values, no-index fallback, and column pruning.
 */
class VcfDataSourceTest {

    static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("VcfDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    @Test
    void schema_matchesVcfSchema() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Dataset<Row> df = spark.read().format("vcf").load(fx.plainVcf().toString());
        assertEquals(VcfSchema.SCHEMA, df.schema());
    }

    // -------------------------------------------------------------------------
    // Full-file count (plain VCF, no index)
    // -------------------------------------------------------------------------

    @Test
    void count_plainVcf_matchesExpected() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf").load(fx.plainVcf().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // Tabix region queries
    // -------------------------------------------------------------------------

    @Test
    void tabixRegionQuery_chromOnly_returnsCorrectCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String bgzPath = fx.bgzVcf().toString();

        long chr1Count = spark.read().format("vcf").load(bgzPath)
                .filter("chrom = 'chr1'")
                .count();
        assertEquals(VcfTestGenerator.VCF_CHR1_COUNT, chr1Count);

        long chr2Count = spark.read().format("vcf").load(bgzPath)
                .filter("chrom = 'chr2'")
                .count();
        assertEquals(VcfTestGenerator.VCF_CHR2_COUNT, chr2Count);
    }

    @Test
    void tabixRegionQuery_chromAndPosRange_returnsCorrectCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String bgzPath = fx.bgzVcf().toString();

        long count = spark.read().format("vcf").load(bgzPath)
                .filter("chrom = 'chr1' AND pos >= 500")
                .count();
        assertEquals(VcfTestGenerator.VCF_CHR1_FROM_500, count);
    }

    @Test
    void tabixRegionQuery_totalCount_matchesAll() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long total = spark.read().format("vcf").load(fx.bgzVcf().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, total);
    }

    // -------------------------------------------------------------------------
    // No-index fallback
    // -------------------------------------------------------------------------

    @Test
    void noIndexFallback_useIndexFalse_returnsAllRecords() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .option("useIndex", "false")
                .load(fx.bgzVcf().toString())
                .count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // Explicit indexPath option
    // -------------------------------------------------------------------------

    @Test
    void explicitIndexPath_worksCorrectly() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .option("indexPath", fx.tbiIndex().toString())
                .load(fx.bgzVcf().toString())
                .filter("chrom = 'chr1'")
                .count();
        assertEquals(VcfTestGenerator.VCF_CHR1_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Field value assertions
    // -------------------------------------------------------------------------

    @Test
    void firstRecord_scalarFields_correct() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Row first = spark.read().format("vcf").load(fx.plainVcf().toString())
                .orderBy("chrom", "pos")
                .first();

        assertEquals("chr1",  first.getString(0));   // chrom
        assertEquals(100,     first.getInt(1));       // pos
        assertEquals("rs1",   first.getString(2));    // id
        assertEquals("A",     first.getString(3));    // ref
        assertEquals("T",     first.getString(4));    // alt
        assertFalse(first.isNullAt(5), "qual should not be null");
        // filter: our test variants have no filter applied
        // info map should not be null
        assertFalse(first.isNullAt(7), "info map should not be null");
    }

    @Test
    void infoMap_containsExpectedKeys() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Row first = spark.read().format("vcf").load(fx.plainVcf().toString())
                .orderBy("chrom", "pos")
                .first();

        @SuppressWarnings("unchecked")
        Map<String, String> info = first.getJavaMap(7);
        assertTrue(info.containsKey("DP"), "info should contain DP");
        assertTrue(info.containsKey("AF"), "info should contain AF");
        assertEquals("30", info.get("DP"));
    }

    @Test
    void genotypesMap_keyedBySampleName() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Row first = spark.read().format("vcf").load(fx.plainVcf().toString())
                .orderBy("chrom", "pos")
                .first();

        // format field
        assertFalse(first.isNullAt(8), "format should not be null");
        assertTrue(first.getString(8).startsWith("GT"), "format should start with GT");

        // genotypes field
        assertFalse(first.isNullAt(9), "genotypes should not be null");
        @SuppressWarnings("unchecked")
        Map<String, String> genotypes = first.getJavaMap(9);
        assertTrue(genotypes.containsKey(VcfTestGenerator.SAMPLE_NAME),
                "genotypes should contain sample1");
        assertTrue(genotypes.get(VcfTestGenerator.SAMPLE_NAME).contains("/"),
                "genotype value should contain allele separator");
    }

    // -------------------------------------------------------------------------
    // BCF format
    // -------------------------------------------------------------------------

    @Test
    void bcfFile_readWithVcfFormat_correctCountAndSchema() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Dataset<Row> df = spark.read().format("vcf").load(fx.bcf().toString());
        assertEquals(VcfSchema.SCHEMA, df.schema());
        assertEquals(VcfTestGenerator.VCF_TOTAL, df.count());
    }

    @Test
    void bcfFile_fieldValues_correctForFirstRecord() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Row first = spark.read().format("vcf").load(fx.bcf().toString())
                .orderBy("chrom", "pos")
                .first();

        assertEquals("chr1", first.getString(0));  // chrom
        assertEquals(100,    first.getInt(1));      // pos
        assertEquals("rs1",  first.getString(2));   // id
        assertEquals("A",    first.getString(3));   // ref
        assertEquals("T",    first.getString(4));   // alt
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_selectSubset_returnsReducedSchema() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Dataset<Row> df = spark.read().format("vcf").load(fx.plainVcf().toString())
                .select("chrom", "pos", "ref", "alt");

        assertEquals(4, df.schema().length());
        assertEquals("chrom", df.schema().apply(0).name());
        assertEquals("pos",   df.schema().apply(1).name());
        assertEquals(VcfTestGenerator.VCF_TOTAL, df.count());
    }
}
