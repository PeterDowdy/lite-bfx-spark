package com.litebfx.fastq;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link FastqDataSource} via {@code spark.read.format("fastq")}.
 *
 * <p>Uses:
 * <ul>
 *   <li>Synthetic fixtures from {@link FastqTestGenerator} (plain + gzipped + no-description).</li>
 *   <li>Real-world {@code TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz} (25 000 records) from
 *       test resources.</li>
 * </ul>
 */
class FastqDataSourceTest {

    static SparkSession spark;
    static FastqTestGenerator.Fixtures fixtures;
    static String realGzipPath;

    /** Lines in the real FASTQ gz fixture: 100 000 lines / 4 = 25 000 reads. */
    static final long REAL_GZ_COUNT = 25_000L;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("FastqDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        fixtures = FastqTestGenerator.generate(tempDir);

        URL realUrl = FastqDataSourceTest.class.getClassLoader()
                .getResource("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz");
        assertNotNull(realUrl, "TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz not found in test resources");
        realGzipPath = java.nio.file.Paths.get(realUrl.toURI()).toUri().toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    // -------------------------------------------------------------------------
    // Schema
    // -------------------------------------------------------------------------

    @Test
    void schema_matchesFastqSchema() {
        StructType schema = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString()).schema();
        assertEquals(FastqSchema.SCHEMA, schema);
        assertEquals(5, schema.length());
        assertEquals("readName",      schema.apply(0).name());
        assertEquals("sequence",      schema.apply(1).name());
        assertEquals("baseQualities", schema.apply(2).name());
        assertEquals("description",   schema.apply(3).name());
        assertEquals("readNumber",    schema.apply(4).name());
    }

    // -------------------------------------------------------------------------
    // Counts
    // -------------------------------------------------------------------------

    @Test
    void count_plainFastq_matchesExpected() {
        long count = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString()).count();
        assertEquals(FastqTestGenerator.PLAIN_COUNT, count);
    }

    @Test
    void count_gzipFastq_matchesExpected() {
        long count = spark.read().format("fastq")
                .load(fixtures.gzipFastq().toString()).count();
        assertEquals(FastqTestGenerator.GZIP_COUNT, count);
    }

    @Test
    void count_realGzipFastq_matches25000() {
        long count = spark.read().format("fastq").load(realGzipPath).count();
        assertEquals(REAL_GZ_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // First record field values (plain)
    // -------------------------------------------------------------------------

    @Test
    void firstRecord_readName_matchesExpected() {
        Row row = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString())
                .orderBy("readName")
                .first();
        assertEquals(FastqTestGenerator.FIRST_READ_NAME, row.getString(row.fieldIndex("readName")));
    }

    @Test
    void firstRecord_sequence_matchesExpected() {
        Row row = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString())
                .orderBy("readName")
                .first();
        assertEquals(FastqTestGenerator.SEQUENCE, row.getString(row.fieldIndex("sequence")));
    }

    @Test
    void firstRecord_baseQualities_matchesExpected() {
        Row row = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString())
                .orderBy("readName")
                .first();
        assertEquals(FastqTestGenerator.BASE_QUALITIES,
                row.getString(row.fieldIndex("baseQualities")));
    }

    @Test
    void firstRecord_description_matchesExpected() {
        Row row = spark.read().format("fastq")
                .load(fixtures.plainFastq().toString())
                .orderBy("readName")
                .first();
        assertEquals(FastqTestGenerator.FIRST_DESCRIPTION,
                row.getString(row.fieldIndex("description")));
    }

    // -------------------------------------------------------------------------
    // No-description fixture
    // -------------------------------------------------------------------------

    @Test
    void description_isNull_whenHeaderHasNoDescription() {
        Dataset<Row> df = spark.read().format("fastq")
                .load(fixtures.noDescFastq().toString());
        assertEquals(FastqTestGenerator.NODESC_COUNT, df.count());
        // All description values must be null
        long nonNull = df.filter("description is not null").count();
        assertEquals(0L, nonNull, "All description fields should be null when header has no description");
    }

    // -------------------------------------------------------------------------
    // Gzipped → single partition
    // -------------------------------------------------------------------------

    @Test
    void gzipFastq_producesCorrectCount() {
        // Gzipped files must be read as a single partition and return all records.
        long count = spark.read().format("fastq")
                .load(fixtures.gzipFastq().toString()).count();
        assertEquals(FastqTestGenerator.GZIP_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Real-world FASTQ header parsing
    // -------------------------------------------------------------------------

    @Test
    void realGzip_readNameDoesNotContainAtSign() {
        // htsjdk strips '@'; readName should not start with '@'
        Row row = spark.read().format("fastq").load(realGzipPath).first();
        String readName = row.getString(row.fieldIndex("readName"));
        assertFalse(readName.startsWith("@"),
                "readName should not include leading '@'");
    }

    @Test
    void realGzip_firstReadName_matchesSampledValue() {
        Row row = spark.read().format("fastq").load(realGzipPath).first();
        String readName = row.getString(row.fieldIndex("readName"));
        // From the fixture: first header is "HISEQ_HU01:89:H7YRLADXX:1:1101:1116:2123"
        assertEquals("HISEQ_HU01:89:H7YRLADXX:1:1101:1116:2123", readName);
    }

    @Test
    void realGzip_firstDescription_matchesSampledValue() {
        Row row = spark.read().format("fastq").load(realGzipPath).first();
        String desc = row.getString(row.fieldIndex("description"));
        // From the fixture: header line is "... 1:N:0:ATCACG"
        assertEquals("1:N:0:ATCACG", desc);
    }

    @Test
    void realGzip_sequenceLengthIsNonZero() {
        Row row = spark.read().format("fastq").load(realGzipPath).first();
        String seq = row.getString(row.fieldIndex("sequence"));
        assertTrue(seq.length() > 0);
    }

    // -------------------------------------------------------------------------
    // R1/R2 read-number detection
    // -------------------------------------------------------------------------

    @Test
    void detectReadNumber_r1Underscore_returns1() {
        assertEquals(Integer.valueOf(1), FastqScan.detectReadNumber("sample_R1_001.fastq.gz"));
    }

    @Test
    void detectReadNumber_r2Underscore_returns2() {
        assertEquals(Integer.valueOf(2), FastqScan.detectReadNumber("sample_R2_001.fastq.gz"));
    }

    @Test
    void detectReadNumber_r1Dot_returns1() {
        assertEquals(Integer.valueOf(1), FastqScan.detectReadNumber("sample_R1.fastq.gz"));
    }

    @Test
    void detectReadNumber_r2Dot_returns2() {
        assertEquals(Integer.valueOf(2), FastqScan.detectReadNumber("sample_R2.fastq.gz"));
    }

    @Test
    void detectReadNumber_numeric1Dot_returns1() {
        assertEquals(Integer.valueOf(1), FastqScan.detectReadNumber("sample_1.fastq.gz"));
    }

    @Test
    void detectReadNumber_numeric2Dot_returns2() {
        assertEquals(Integer.valueOf(2), FastqScan.detectReadNumber("sample_2.fastq.gz"));
    }

    @Test
    void detectReadNumber_zeroPadded01_returns1() {
        assertEquals(Integer.valueOf(1), FastqScan.detectReadNumber("sample_01.fastq.gz"));
    }

    @Test
    void detectReadNumber_zeroPaddedR02Underscore_returns2() {
        assertEquals(Integer.valueOf(2), FastqScan.detectReadNumber("sample_R02_001.fastq.gz"));
    }

    @Test
    void detectReadNumber_noPattern_returnsNull() {
        assertNull(FastqScan.detectReadNumber("sample.fastq.gz"));
    }

    @Test
    void readNumber_isSetOnRealGzipFile() {
        // The real fixture filename contains "_R1_" so readNumber must be 1
        Row row = spark.read().format("fastq").load(realGzipPath).first();
        assertEquals(1, row.getInt(row.fieldIndex("readNumber")));
    }

    @Test
    void readNumber_directoryLoad_setsR1andR2() throws Exception {
        // Point at the directory containing all four real FASTQ gz files (L001 R1/R2, L002 R1/R2).
        URL dirUrl = FastqDataSourceTest.class.getClassLoader()
                .getResource("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz");
        assertNotNull(dirUrl);
        String dirPath = java.nio.file.Paths.get(dirUrl.toURI()).getParent().toUri().toString();

        Dataset<Row> df = spark.read().format("fastq").load(dirPath);
        List<Row> r1Rows = df.filter("readNumber = 1").limit(1).collectAsList();
        List<Row> r2Rows = df.filter("readNumber = 2").limit(1).collectAsList();
        assertFalse(r1Rows.isEmpty(), "Expected rows with readNumber=1 (R1 files)");
        assertFalse(r2Rows.isEmpty(), "Expected rows with readNumber=2 (R2 files)");
    }

    // -------------------------------------------------------------------------
    // Error conditions
    // -------------------------------------------------------------------------

    @Test
    void missingFile_throwsException() {
        assertThrows(Exception.class, () ->
            spark.read().format("fastq").load("/no/such/file.fastq").count());
    }
}
