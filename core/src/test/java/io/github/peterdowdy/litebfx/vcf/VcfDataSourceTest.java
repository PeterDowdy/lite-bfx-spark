package io.github.peterdowdy.litebfx.vcf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
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

        assertEquals("chr1",  first.getString(0));               // chrom
        assertEquals(100,     first.getInt(1));                  // pos
        assertEquals("rs1",   first.getString(2));               // id
        assertEquals("A",     first.getString(3));               // ref
        assertEquals("T",     first.<String>getList(4).get(0)); // alt[0]
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

        assertEquals("chr1", first.getString(0));               // chrom
        assertEquals(100,    first.getInt(1));                  // pos
        assertEquals("rs1",  first.getString(2));               // id
        assertEquals("A",    first.getString(3));               // ref
        assertEquals("T",    first.<String>getList(4).get(0)); // alt[0]
    }

    // -------------------------------------------------------------------------
    // Per-chromosome tabix partitioning (bgzipped VCF, no filter pushed)
    // -------------------------------------------------------------------------

    @Test
    void tabixFullScan_createsOnePartitionPerChrom() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);

        // Build a VcfScan directly so we can inspect the partition plan.
        Map<String, String> opts = Map.of("path", fx.bgzVcf().toString());
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(opts),
                VcfSchema.SCHEMA,
                null, 1, Integer.MAX_VALUE);
        InputPartition[] parts = scan.planInputPartitions();

        assertEquals(VcfTestGenerator.VCF_CHROM_COUNT, parts.length,
                "should create one partition per chromosome when tabix is present and no filter is pushed");
        for (InputPartition p : parts) {
            VcfInputPartition vp = (VcfInputPartition) p;
            assertNotNull(vp.getQueryChroms(), "each partition should have a queryChroms array");
            assertEquals(1, vp.getQueryChroms().length,
                    "each chromosome group should contain exactly one chrom in this small test");
        }
    }

    @Test
    void tabixFullScan_multiPartition_correctTotalCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf").load(fx.bgzVcf().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                "per-chromosome partitions should return the complete record set");
    }

    @Test
    void tabixFullScan_numPartitions1_singlePartition() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);

        Map<String, String> opts = Map.of(
                "path", fx.bgzVcf().toString(),
                "numPartitions", "1");
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(opts),
                VcfSchema.SCHEMA,
                null, 1, Integer.MAX_VALUE);
        InputPartition[] parts = scan.planInputPartitions();

        assertEquals(1, parts.length, "numPartitions=1 should collapse all chroms into one partition");
        VcfInputPartition vp = (VcfInputPartition) parts[0];
        assertNotNull(vp.getQueryChroms());
        assertEquals(VcfTestGenerator.VCF_CHROM_COUNT, vp.getQueryChroms().length);
    }

    @Test
    void tabixFullScan_numPartitions1_correctCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .option("numPartitions", "1")
                .load(fx.bgzVcf().toString())
                .count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // Line-split partitions (plain uncompressed VCF)
    // -------------------------------------------------------------------------

    @Test
    void splitPartitions_correctCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .option("vcfSplitSize", "50")  // force many small partitions
                .load(fx.plainVcf().toString())
                .count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count);
    }

    @Test
    void splitPartitions_fieldValues_correct() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Row first = spark.read().format("vcf")
                .option("vcfSplitSize", "50")
                .load(fx.plainVcf().toString())
                .orderBy("chrom", "pos")
                .first();

        assertEquals("chr1", first.getString(0));               // chrom
        assertEquals(100,    first.getInt(1));                  // pos
        assertEquals("rs1",  first.getString(2));               // id
        assertEquals("A",    first.getString(3));               // ref
        assertEquals("T",    first.<String>getList(4).get(0)); // alt[0]
        assertFalse(first.isNullAt(5), "qual should not be null");
        assertFalse(first.isNullAt(7), "info map should not be null");

        @SuppressWarnings("unchecked")
        Map<String, String> info = first.getJavaMap(7);
        assertTrue(info.containsKey("DP"), "info should contain DP");
        assertEquals("30", info.get("DP"));

        assertFalse(first.isNullAt(8), "format should not be null");
        assertTrue(first.getString(8).startsWith("GT"), "format should start with GT");

        assertFalse(first.isNullAt(9), "genotypes should not be null");
        @SuppressWarnings("unchecked")
        Map<String, String> genotypes = first.getJavaMap(9);
        assertTrue(genotypes.containsKey(VcfTestGenerator.SAMPLE_NAME),
                "genotypes map should be keyed by sample name");
        assertTrue(genotypes.get(VcfTestGenerator.SAMPLE_NAME).contains("/"),
                "genotype value should contain allele separator");
    }

    // -------------------------------------------------------------------------
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_countMatchesExpected() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW vcf_view USING vcf"
                + " OPTIONS (path '" + fx.bgzVcf() + "')");
        long count = spark.sql("SELECT count(*) FROM vcf_view").first().getLong(0);
        assertEquals(VcfTestGenerator.VCF_TOTAL, count);
        spark.sql("DROP VIEW IF EXISTS vcf_view");
    }

    @Test
    void sql_createTempView_regionFilter_returnsCorrectCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW vcf_region_view USING vcf"
                + " OPTIONS (path '" + fx.bgzVcf() + "')");
        long count = spark.sql(
                "SELECT count(*) FROM vcf_region_view WHERE chrom = 'chr1'")
                .first().getLong(0);
        assertEquals(VcfTestGenerator.VCF_CHR1_COUNT, count);
        spark.sql("DROP VIEW IF EXISTS vcf_region_view");
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

    // -------------------------------------------------------------------------
    // Input validation (VcfScan)
    // -------------------------------------------------------------------------

    @Test
    void vcfSplitSize_zero_throwsIllegalArgument() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        assertThrows(Exception.class, () ->
                spark.read().format("vcf")
                        .option("vcfSplitSize", "0")
                        .load(fx.plainVcf().toString())
                        .count());
    }

    @Test
    void numPartitions_zero_throwsIllegalArgument() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        assertThrows(Exception.class, () ->
                spark.read().format("vcf")
                        .option("numPartitions", "0")
                        .load(fx.bgzVcf().toString())
                        .count());
    }

    // -------------------------------------------------------------------------
    // Split-mode edge cases: dot fields, flag INFO, sites-only, PASS filter
    // -------------------------------------------------------------------------

    /** Writes a minimal plain-text VCF to a temp file and returns its URI string. */
    private String writePlainVcf(String filename, String content) throws Exception {
        Path p = tempDir.resolve(filename);
        Files.writeString(p, content, StandardCharsets.UTF_8);
        return p.toUri().toString();
    }

    @Test
    void splitMode_dotId_dotAlt_dotQual_returnNullValues() throws Exception {
        String path = writePlainVcf("dots.vcf",
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\t.\t.\t.\tDP=10\n");

        Row row = spark.read().format("vcf").load(path).first();

        assertTrue(row.isNullAt(2),  "id '.' should be null");
        assertTrue(row.isNullAt(4),  "alt '.' should be null");
        assertTrue(row.isNullAt(5),  "qual '.' should be null");
        assertTrue(row.isNullAt(6),  "filter '.' should be null");
    }

    @Test
    void splitMode_infoFlagField_encodedAsTrue() throws Exception {
        String path = writePlainVcf("flags.vcf",
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\trs1\tA\tT\t30\tPASS\tSOMATIC;DP=10\n");

        Row row = spark.read().format("vcf").load(path).first();

        @SuppressWarnings("unchecked")
        Map<String, String> info = row.getJavaMap(7);
        assertEquals("true", info.get("SOMATIC"), "flag field should map to 'true'");
        assertEquals("10",   info.get("DP"));
    }

    @Test
    void splitMode_passFilter_returnsPassString() throws Exception {
        String path = writePlainVcf("pass.vcf",
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\trs1\tA\tT\t30\tPASS\tDP=10\n");

        Row row = spark.read().format("vcf").load(path).first();

        assertEquals("PASS", row.getString(6), "non-dot filter should be preserved as-is");
    }

    @Test
    void splitMode_sitesOnly_nullFormatAndGenotypes() throws Exception {
        // Sites-only VCF: only 8 mandatory columns, no FORMAT or sample columns.
        String path = writePlainVcf("sites.vcf",
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\trs1\tA\tT\t30\tPASS\tDP=10\n" +
                "chr1\t200\trs2\tC\tG\t20\t.\tDP=5\n");

        Dataset<Row> df = spark.read().format("vcf").load(path);
        assertEquals(2L, df.count());

        Row row = df.orderBy("pos").first();
        assertTrue(row.isNullAt(8), "format should be null for sites-only VCF");
        assertTrue(row.isNullAt(9), "genotypes should be null for sites-only VCF");
    }

    @Test
    void splitMode_emptyInfoDot_returnsEmptyMap() throws Exception {
        String path = writePlainVcf("emptyinfo.vcf",
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\trs1\tA\tT\t30\tPASS\t.\n");

        Row row = spark.read().format("vcf").load(path).first();

        @SuppressWarnings("unchecked")
        Map<String, String> info = row.getJavaMap(7);
        assertTrue(info.isEmpty(), "INFO '.' should produce an empty map");
    }

    // -------------------------------------------------------------------------
    // VCFFileReader edge cases: null alt, null qual, Boolean INFO, List INFO,
    // PASS filter, non-empty filter string
    // -------------------------------------------------------------------------

    @Test
    void vcfReader_passFilter_returnsPass() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row first = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").first();  // pos=100, PASS filter
        assertEquals("PASS", first.getString(6), "PASS filter should be 'PASS'");
    }

    @Test
    void vcfReader_nonEmptyFilter_returnsFilterString() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row second = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").collectAsList().get(1);  // pos=200, LowQual filter
        assertEquals("LowQual", second.getString(6), "non-empty filter should be returned as-is");
    }

    @Test
    void vcfReader_noAltAlleles_altIsNull() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row second = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").collectAsList().get(1);  // pos=200, ref-only
        assertTrue(second.isNullAt(4), "ref-only site should have null alt");
    }

    @Test
    void vcfReader_noQual_qualIsNull() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row second = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").collectAsList().get(1);  // pos=200, no qual
        assertTrue(second.isNullAt(5), "site with no QUAL should have null qual");
    }

    @Test
    void vcfReader_booleanInfoFlag_encodedAsTrue() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row first = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").first();  // pos=100, SOMATIC=true flag
        @SuppressWarnings("unchecked")
        Map<String, String> info = first.getJavaMap(7);
        assertEquals("true", info.get("SOMATIC"), "Boolean flag INFO should encode as 'true'");
    }

    @Test
    void vcfReader_listInfoField_commaSeparated() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        Row first = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").first();  // pos=100, DP4=[1,2,3,4]
        @SuppressWarnings("unchecked")
        Map<String, String> info = first.getJavaMap(7);
        assertEquals("1,2,3,4", info.get("DP4"), "List INFO should be comma-separated");
    }

    @Test
    void vcfReader_edgeCases_correctTotalCount() throws Exception {
        VcfTestGenerator.EdgeCaseFixtures fx = VcfTestGenerator.generateEdgeCases(tempDir);
        long count = spark.read().format("vcf").load(fx.bgzVcf().toString()).count();
        assertEquals(VcfTestGenerator.EDGE_TOTAL, count);
    }

    // -------------------------------------------------------------------------
    // Index resolution: .csi fallback
    // -------------------------------------------------------------------------

    @Test
    void csiIndex_fallback_readsAllRecords() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        // Place a copy of the bgz file alongside a .csi index (no .tbi).
        // resolveIndexPath() should fall through to the .csi path.
        Path bgzCopy = tempDir.resolve("csi_test.vcf.gz");
        Path csiPath  = tempDir.resolve("csi_test.vcf.gz.csi");
        Files.copy(Paths.get(fx.bgzVcf()), bgzCopy, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(fx.tbiIndex()), csiPath, StandardCopyOption.REPLACE_EXISTING);

        long count = spark.read().format("vcf").load(bgzCopy.toUri().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                ".csi index should be discovered and used when no .tbi is present");
    }

    // -------------------------------------------------------------------------
    // Index resolution: explicit indexPath that doesn't exist falls back to co-located
    // -------------------------------------------------------------------------

    @Test
    void explicitIndexPath_nonExistent_fallsBackToColocated() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        // Point indexPath at a non-existent file; auto-discovery should find the .tbi.
        long count = spark.read().format("vcf")
                .option("indexPath", "/nonexistent/path/index.tbi")
                .load(fx.bgzVcf().toString())
                .count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                "non-existent explicit indexPath should fall back to co-located .tbi discovery");
    }

    // -------------------------------------------------------------------------
    // .bgz extension treated as bgzipped (not plain-text)
    // -------------------------------------------------------------------------

    @Test
    void bgzExtension_treatedAsBgzipped_correctCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        // Rename .vcf.gz to .vcf.bgz — isPlainTextVcf() must return false for .bgz.
        Path bgzCopy = tempDir.resolve("renamed.vcf.bgz");
        Path tbiCopy  = tempDir.resolve("renamed.vcf.bgz.tbi");
        Files.copy(Paths.get(fx.bgzVcf()), bgzCopy, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(fx.tbiIndex()), tbiCopy, StandardCopyOption.REPLACE_EXISTING);

        long count = spark.read().format("vcf").load(bgzCopy.toUri().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                ".bgz extension should be treated as bgzipped VCF, not plain-text");
    }

    // -------------------------------------------------------------------------
    // groupChroms: extra > 0 (3 chroms, 2 groups)
    // -------------------------------------------------------------------------

    @Test
    void groupChroms_extraDistributed_correctPartitionLayout() throws Exception {
        VcfTestGenerator.TripleChromFixtures fx = VcfTestGenerator.generateTripleChrom(tempDir);
        // 3 chroms, numPartitions=2 → groupChroms(3, 2): base=1, extra=1
        // → group 0 gets 2 chroms, group 1 gets 1 chrom
        Map<String, String> opts = Map.of(
                "path", fx.bgzVcf().toString(),
                "numPartitions", "2");
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(opts),
                VcfSchema.SCHEMA,
                null, 1, Integer.MAX_VALUE);
        InputPartition[] parts = scan.planInputPartitions();

        assertEquals(2, parts.length, "3 chroms with numPartitions=2 should produce 2 partitions");
        assertEquals(2, ((VcfInputPartition) parts[0]).getQueryChroms().length,
                "first group should receive the extra chrom (size = base+1 = 2)");
        assertEquals(1, ((VcfInputPartition) parts[1]).getQueryChroms().length,
                "second group should have base size = 1");
    }

    @Test
    void groupChroms_extraDistributed_correctTotalCount() throws Exception {
        VcfTestGenerator.TripleChromFixtures fx = VcfTestGenerator.generateTripleChrom(tempDir);
        long count = spark.read().format("vcf")
                .option("numPartitions", "2")
                .load(fx.bgzVcf().toString())
                .count();
        assertEquals(3L, count, "3 variants across 3 chroms should all be returned");
    }

    // -------------------------------------------------------------------------
    // .bgzf extension treated as bgzipped — covers isPlainTextVcf() .bgzf check
    // -------------------------------------------------------------------------

    @Test
    void bgzfExtension_treatedAsBgzipped_correctCount() throws Exception {
        // Rename .vcf.gz to .vcf.bgzf — isPlainTextVcf() evaluates four checks:
        // !endsWith(.gz) [true] && !endsWith(.bgz) [true] && !endsWith(.bgzf) [false].
        // The .bgzf check returning false is the one previously-uncovered branch.
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Path bgzfCopy = tempDir.resolve("renamed2.vcf.bgzf");
        Path tbiCopy  = tempDir.resolve("renamed2.vcf.bgzf.tbi");
        Files.copy(Paths.get(fx.bgzVcf()),   bgzfCopy, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(fx.tbiIndex()), tbiCopy,  StandardCopyOption.REPLACE_EXISTING);

        long count = spark.read().format("vcf").load(bgzfCopy.toUri().toString()).count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                ".bgzf extension should be treated as bgzipped VCF, not plain-text");
    }

    // -------------------------------------------------------------------------
    // Multi-sample VCF: genotype coverage (GT:AD:DP:GQ:PL, 3 samples)
    // -------------------------------------------------------------------------

    @Test
    void multiSample_allSamplesAppearAsGenotypeKeys() throws Exception {
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        Row first = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").first();  // chr1:100 biallelic

        assertFalse(first.isNullAt(9), "genotypes map should not be null");
        Map<String, String> genotypes = first.getJavaMap(9);
        assertEquals(3, genotypes.size(), "should have one entry per sample");
        for (String sample : VcfTestGenerator.MULTI_SAMPLE_NAMES) {
            assertTrue(genotypes.containsKey(sample), "missing sample: " + sample);
        }
    }

    @Test
    void multiSample_genotypeStringsReflectActualGt() throws Exception {
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        Row first = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").first();  // chr1:100

        // VCFFileReader encodes GT via Genotype.getGenotypeString(); htsjdk appends
        // '*' to reference allele display strings (e.g. Allele "A" ref → "A*").
        Map<String, String> genotypes = first.getJavaMap(9);
        assertEquals("A*/T",   genotypes.get("NA12878"), "NA12878 should be het (A*/T)");
        assertEquals("A*/A*",  genotypes.get("NA12877"), "NA12877 should be hom-ref (A*/A*)");
        assertEquals("T/T",    genotypes.get("NA12879"), "NA12879 should be hom-alt (T/T)");
    }

    @Test
    void multiSample_multiAllelicSite_altIsArray() throws Exception {
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        List<Row> rows = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").collectAsList();
        assertEquals(VcfTestGenerator.MULTI_SAMPLE_VARIANT_COUNT, rows.size());

        Row multiAllelic = rows.get(1);  // chr1:500 → alt = [T, G]
        assertFalse(multiAllelic.isNullAt(4), "multi-allelic site should have non-null alt");
        List<String> alt = multiAllelic.getList(4);
        assertEquals(2, alt.size(), "bi-allelic alt column should have 2 elements");
        assertTrue(alt.contains("T") && alt.contains("G"),
                "alt should contain both T and G, got: " + alt);
    }

    @Test
    void multiSample_multiAllelicSite_allSamplesHaveDifferentGt() throws Exception {
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        List<Row> rows = spark.read().format("vcf").load(fx.bgzVcf().toString())
                .orderBy("pos").collectAsList();

        // VCFFileReader encodes GT as allele display bases; ref alleles get '*' suffix.
        Map<String, String> genotypes = rows.get(1).getJavaMap(9);  // chr1:500
        assertEquals("A*/T",  genotypes.get("NA12878"), "NA12878 should be 0/1 (A*/T)");
        assertEquals("A*/G",  genotypes.get("NA12877"), "NA12877 should be 0/2 (A*/G)");
        assertEquals("G/T",   genotypes.get("NA12879"), "NA12879 should be 1/2 (G/T)");
    }

    @Test
    void multiSample_splitMode_allSamplesPresent() throws Exception {
        // vcfSplitSize forces the byte-range split code path (getSplitMode / buildGenotypesFromColumns)
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        Row first = spark.read().format("vcf")
                .option("vcfSplitSize", "50")
                .load(fx.plainVcf().toString())
                .orderBy("pos").first();

        assertFalse(first.isNullAt(9), "genotypes should not be null in split mode");
        Map<String, String> genotypes = first.getJavaMap(9);
        assertEquals(3, genotypes.size(), "split mode should expose all 3 samples");
        for (String sample : VcfTestGenerator.MULTI_SAMPLE_NAMES) {
            assertTrue(genotypes.containsKey(sample), "split mode missing sample: " + sample);
        }
    }

    @Test
    void multiSample_splitMode_genotypeValuesPreserveAllFormatFields() throws Exception {
        // Split mode stores the raw VCF column verbatim: numeric GT:AD:DP:GQ:PL.
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        Row first = spark.read().format("vcf")
                .option("vcfSplitSize", "50")
                .load(fx.plainVcf().toString())
                .orderBy("pos").first();  // chr1:100

        Map<String, String> genotypes = first.getJavaMap(9);
        String na12878 = genotypes.get("NA12878");
        assertTrue(na12878.startsWith("0/1"),
                "split-mode GT should use numeric allele indices, got: " + na12878);
        String[] parts = na12878.split(":");
        assertTrue(parts.length >= 5,
                "genotype should encode GT:AD:DP:GQ:PL (5+ fields), got: " + na12878);
        assertTrue(na12878.contains("15,15"), "AD=15,15 should be present in raw column");
        assertTrue(na12878.contains("30"),    "DP=30 should be present in raw column");
    }

    @Test
    void multiSample_splitMode_formatColumnContainsAllKeys() throws Exception {
        // In split mode the FORMAT column is the raw colon-joined string from the file.
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        Row first = spark.read().format("vcf")
                .option("vcfSplitSize", "50")
                .load(fx.plainVcf().toString())
                .orderBy("pos").first();

        assertFalse(first.isNullAt(8), "format column should not be null");
        String format = first.getString(8);
        for (String key : new String[]{"GT", "AD", "DP", "GQ", "PL"}) {
            assertTrue(format.contains(key), "format should contain " + key + ", got: " + format);
        }
    }

    @Test
    void multiSample_totalVariantCount_correct() throws Exception {
        VcfTestGenerator.MultiSampleFixtures fx = VcfTestGenerator.generateMultiSample(tempDir);
        long count = spark.read().format("vcf").load(fx.bgzVcf().toString()).count();
        assertEquals(VcfTestGenerator.MULTI_SAMPLE_VARIANT_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // planInputPartitions — pathStr == null throws IllegalArgumentException
    // -------------------------------------------------------------------------

    @Test
    void vcfScan_noPathOption_throwsIllegalArgument() {
        // Instantiate VcfScan with an empty options map (no "path" key) and verify
        // planInputPartitions() throws before reaching the SparkSession call.
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(Map.of()),
                VcfSchema.SCHEMA, null, 1, Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, scan::planInputPartitions);
    }

    // -------------------------------------------------------------------------
    // planInputPartitions — readChromsFromTabix returns null (bad .tbi)
    // → chroms != null && !chroms.isEmpty() evaluates false → single partition
    // -------------------------------------------------------------------------

    @Test
    void tabixFullScan_badIndex_readChromsFails_fallsBackToSinglePartition() throws Exception {
        // Place the bad "index" at a separate path (not co-located with the VCF) and
        // pass it via option("indexPath").  resolveIndexPath() returns it; readChromsFromTabix()
        // catches the parse error and returns null (chroms == null → false branch); planInputPartitions
        // falls back to a single full-scan partition.  VcfPartitionReader.open() then uses
        // VCFFileReader(path, requireIndex=false), which auto-discovers the valid co-located .tbi
        // and reads all records — the bad file is never touched by the reader because queryChrom is null.
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);

        Path badIdx = tempDir.resolve("bad_noncolocated.tbi");
        Files.writeString(badIdx, "not a tabix index\n", StandardCharsets.UTF_8);

        long count = spark.read().format("vcf")
                .option("indexPath", badIdx.toUri().toString())
                .load(fx.bgzVcf().toString())
                .count();
        assertEquals(VcfTestGenerator.VCF_TOTAL, count,
                "bad explicit indexPath should fall back to full-scan returning all records");
    }

    // -------------------------------------------------------------------------
    // Statistics (SupportsReportStatistics)
    // -------------------------------------------------------------------------

    @Test
    void estimateStatistics_sizeInBytes_greaterThanZero() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        Dataset<Row> df = spark.read().format("vcf").load(fx.plainVcf().toString());
        long sizeBytes = df.queryExecution().optimizedPlan()
                .stats().sizeInBytes().longValue();
        assertTrue(sizeBytes > 0, "sizeInBytes should be > 0 for a non-empty VCF file");
        assertTrue(sizeBytes <= new java.io.File(fx.plainVcf()).length() * 2,
                "sizeInBytes should be within 2x of the actual file size");
    }

    // -------------------------------------------------------------------------
    // Limit pushdown (SupportsPushDownLimit)
    // -------------------------------------------------------------------------

    @Test
    void limit_pushdown_returnsExactCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .load(fx.plainVcf().toString())
                .limit(2)
                .count();
        assertEquals(2L, count, "limit(2) should return exactly 2 rows");
    }

    @Test
    void limit_withChromFilter_returnsCorrectRows() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        long count = spark.read().format("vcf")
                .load(fx.plainVcf().toString())
                .filter("chrom = 'chr1'")
                .limit(2)
                .count();
        assertTrue(count <= 2L, "chrom filter + limit(2) should return at most 2 rows");
        assertTrue(count > 0L, "chrom filter + limit(2) should return at least 1 row");
    }

    // -------------------------------------------------------------------------
    // Ordering (SupportsReportOrdering)
    // -------------------------------------------------------------------------

    @Test
    void outputOrdering_tabixIndexed_returnsChromAndPos() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(Map.of("path", fx.bgzVcf().toString())),
                VcfSchema.SCHEMA, null, 1, Integer.MAX_VALUE);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(2, ordering.length, "tabix-indexed VCF should report 2 ordering fields");
        assertEquals("chrom", ((NamedReference) ordering[0].expression()).fieldNames()[0]);
        assertEquals("pos",   ((NamedReference) ordering[1].expression()).fieldNames()[0]);
    }

    @Test
    void outputOrdering_plainVcf_isEmpty() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        VcfScan scan = new VcfScan(
                new CaseInsensitiveStringMap(Map.of("path", fx.plainVcf().toString())),
                VcfSchema.SCHEMA, null, 1, Integer.MAX_VALUE);
        SortOrder[] ordering = scan.outputOrdering();
        assertEquals(0, ordering.length, "plain VCF without index should report no ordering");
    }
}
