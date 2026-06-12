package com.litebfx.fasta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link FastaDataSource} via {@code spark.read.format("fasta")}.
 *
 * <p>Uses {@code realn01.fa} + {@code realn01.fa.fai} from test resources:
 * 1 contig ({@code 000000F}), length 686.
 */
class FastaDataSourceTest {

    static SparkSession spark;
    static String fastaPath;
    static String faiPath;

    @TempDir
    static Path tempDir;

    /** Sequence read from the raw file for byte-for-byte comparison. */
    static final String EXPECTED_CONTIG = "000000F";
    static final long EXPECTED_LENGTH = 686L;
    // First 60 bases of 000000F from realn01.fa (line 2)
    static final String EXPECTED_PREFIX =
        "CAGACAAACATACACCATCAGACAGCAGCACCATATTCTTTTTTTCTGCTAATTTGCTAA";

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("FastaDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        URL faUrl = FastaDataSourceTest.class.getClassLoader().getResource("realn01.fa");
        assertNotNull(faUrl, "realn01.fa not found in test resources");
        fastaPath = java.nio.file.Paths.get(faUrl.toURI()).toUri().toString();

        URL faiUrl = FastaDataSourceTest.class.getClassLoader().getResource("realn01.fa.fai");
        assertNotNull(faiUrl, "realn01.fa.fai not found in test resources");
        faiPath = java.nio.file.Paths.get(faiUrl.toURI()).toUri().toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    @Test
    void schema_matchesFastaSchema() {
        StructType schema = spark.read().format("fasta").load(fastaPath).schema();
        assertEquals(FastaSchema.SCHEMA, schema);
        assertEquals(3, schema.length());
        assertEquals("name",     schema.apply(0).name());
        assertEquals("sequence", schema.apply(1).name());
        assertEquals("length",   schema.apply(2).name());
    }

    // -------------------------------------------------------------------------
    // Contig count (FAI-based: one partition per contig)
    // -------------------------------------------------------------------------

    @Test
    void count_withFai_matchesContigCount() {
        long count = spark.read().format("fasta").load(fastaPath).count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Length from FAI
    // -------------------------------------------------------------------------

    @Test
    void length_matchesFaiLengthColumn() {
        Row row = spark.read().format("fasta").load(fastaPath)
                .filter("name = '" + EXPECTED_CONTIG + "'")
                .first();
        assertEquals(EXPECTED_LENGTH, row.getLong(row.fieldIndex("length")));
    }

    // -------------------------------------------------------------------------
    // Sequence content
    // -------------------------------------------------------------------------

    @Test
    void sequence_prefixMatchesRawFile() {
        Row row = spark.read().format("fasta").load(fastaPath)
                .filter("name = '" + EXPECTED_CONTIG + "'")
                .first();
        String seq = row.getString(row.fieldIndex("sequence"));
        assertEquals(EXPECTED_LENGTH, seq.length());
        assertTrue(seq.startsWith(EXPECTED_PREFIX),
                "Sequence should start with expected prefix");
    }

    // -------------------------------------------------------------------------
    // No-FAI fallback (explicit indexPath suppressed via a path that has no .fai)
    // -------------------------------------------------------------------------

    @Test
    void noFaiFallback_explicitIndexPath_usesProvidedIndex() {
        // Explicit indexPath should resolve correctly and still return 1 contig
        long count = spark.read().format("fasta")
                .option("indexPath", faiPath)
                .load(fastaPath)
                .count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_filterByName_returnsCorrectLength() {
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW fasta_view USING fasta"
                + " OPTIONS (path '" + fastaPath + "')");
        long length = spark.sql(
                "SELECT length FROM fasta_view WHERE name = '" + EXPECTED_CONTIG + "'")
                .first().getLong(0);
        assertEquals(EXPECTED_LENGTH, length);
        spark.sql("DROP VIEW IF EXISTS fasta_view");
    }

    // -------------------------------------------------------------------------
    // No-FAI fallback — single full-scan partition
    // Covers the faiPath == null branch in FastaScan.planInputPartitions()
    // -------------------------------------------------------------------------

    @Test
    void noFaiFallback_noIndex_singlePartitionScan() throws Exception {
        // Copy realn01.fa to a temp path without a co-located .fai and with no
        // explicit indexPath option — resolveFaiPath() returns null → single
        // full-scan partition (the faiPath == null else-branch).
        Path fastaNoIdx = tempDir.resolve("noidx.fa");
        Files.copy(Paths.get(java.net.URI.create(fastaPath)), fastaNoIdx);
        // Do NOT write a .fai alongside it

        long count = spark.read().format("fasta")
                .load(fastaNoIdx.toUri().toString())
                .count();
        // Full-scan reads all contigs; realn01.fa has 1 contig
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Explicit non-existent indexPath — resolveFaiPath() falls through to co-located
    // Covers the explicit != null && !exists(p) branch
    // -------------------------------------------------------------------------

    @Test
    void explicitIndexPath_nonExistentFile_fallsBackToColocated() throws Exception {
        // Provide an explicit indexPath that doesn't exist → resolveFaiPath() falls
        // through to the co-located .fai check (which succeeds for realn01.fa).
        String badIndexPath = tempDir.resolve("no_such.fai").toUri().toString();

        long count = spark.read().format("fasta")
                .option("indexPath", badIndexPath)
                .load(fastaPath)
                .count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // maxMergeGap option — exercises the maxMergeGapOpt != null branch
    // -------------------------------------------------------------------------

    @Test
    void maxMergeGap_customValue_readsCorrectly() {
        // Setting maxMergeGap triggers the maxMergeGapOpt != null branch in
        // planInputPartitions(), exercising the parseInt + assignment path.
        long count = spark.read().format("fasta")
                .option("maxMergeGap", "0")
                .load(fastaPath)
                .count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Pushed-name filter that matches nothing — empty partition array
    // -------------------------------------------------------------------------

    @Test
    void pushedNameFilter_noMatch_returnsEmpty() {
        // Filter by a contig name that does not exist in the FASTA — the pushed
        // name filter removes all contig names from the list, resulting in zero
        // partitions and zero rows.
        long count = spark.read().format("fasta")
                .load(fastaPath)
                .filter("name = 'nonexistent_contig'")
                .count();
        assertEquals(0L, count);
    }

    // -------------------------------------------------------------------------
    // readContigNames — FAI file with an empty line (skipped)
    // Covers the line.isEmpty() branch in readContigNames()
    // -------------------------------------------------------------------------

    @Test
    void readContigNames_faiWithEmptyLine_emptyLineSkipped() throws Exception {
        // Strategy: provide two FAI files.
        //   • co-located  <fa>.fai  — valid, no blank lines; htsjdk reads this when
        //     FastaPartitionReader.openWithNio() auto-discovers the index.
        //   • explicit    blank.fai — has a blank line before the entry; FastaScan
        //     reads this via the "indexPath" option in readContigNames().
        // The blank line triggers line.isEmpty() = true → skip in readContigNames().
        // htsjdk never sees the blank-line FAI so it does not reject the file.
        Path fa           = tempDir.resolve("emptyline.fa");
        Path colocatedFai = tempDir.resolve("emptyline.fa.fai");
        Path blankLineFai = tempDir.resolve("emptyline_blank.fai");

        String seq60    = "ACGT".repeat(15);
        String faContent = ">chr1\n" + seq60 + "\n";
        Files.writeString(fa, faContent, StandardCharsets.UTF_8);

        long offset = (">chr1\n").length(); // 6 bytes to first base
        // Valid FAI for htsjdk (co-located, no blank lines)
        Files.writeString(colocatedFai,
                "chr1\t60\t" + offset + "\t60\t61\n",
                StandardCharsets.UTF_8);
        // FAI with blank line for FastaScan.readContigNames()
        Files.writeString(blankLineFai,
                "\nchr1\t60\t" + offset + "\t60\t61\n",
                StandardCharsets.UTF_8);

        // indexPath → FastaScan.readContigNames() reads blankLineFai (blank line skipped).
        // FastaPartitionReader.openWithNio() uses htsjdk which auto-discovers emptyline.fa.fai.
        long count = spark.read().format("fasta")
                .option("indexPath", blankLineFai.toUri().toString())
                .load(fa.toUri().toString())
                .count();
        assertEquals(1L, count);
    }

    // -------------------------------------------------------------------------
    // Statistics (SupportsReportStatistics)
    // -------------------------------------------------------------------------

    @Test
    void estimateStatistics_indexedFasta_sizeInBytesAndNumRows() {
        Dataset<Row> df = spark.read().format("fasta").load(fastaPath);
        var planStats = df.queryExecution().optimizedPlan().stats();
        long sizeBytes = planStats.sizeInBytes().longValue();
        assertTrue(sizeBytes > 0, "sizeInBytes should be > 0 for a non-empty FASTA file");
        assertTrue(sizeBytes <= new java.io.File(java.nio.file.Paths.get(
                java.net.URI.create(fastaPath)).toString()).length() * 2,
                "sizeInBytes should be within 2x of file size");
        // FAI has 1 contig (000000F) → numRows should be reported as 1
        assertTrue(planStats.rowCount().isDefined(),
                "rowCount should be defined for FAI-indexed FASTA");
        assertEquals(1L, ((Number) planStats.rowCount().get()).longValue(),
                "numRows should equal FAI contig count (1 for realn01.fa)");
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_selectNameAndLength_succeeds() {
        Dataset<Row> df = spark.read().format("fasta").load(fastaPath)
                .select("name", "length");
        assertEquals(2, df.schema().length());
        assertEquals(1L, df.count());
        Row row = df.first();
        assertEquals(EXPECTED_CONTIG, row.getString(0));
        assertEquals(EXPECTED_LENGTH, row.getLong(1));
    }

    // -------------------------------------------------------------------------
    // Limit pushdown (SupportsPushDownLimit)
    // -------------------------------------------------------------------------

    @Test
    void limit_pushdown_returnsExactCount() {
        long count = spark.read().format("fasta").load(fastaPath).limit(1).count();
        assertEquals(1L, count, "limit(1) should return exactly 1 row");
    }
}
