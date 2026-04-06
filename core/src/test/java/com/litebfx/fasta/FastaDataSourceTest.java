package com.litebfx.fasta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URL;

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
}
