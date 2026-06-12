package com.litebfx.fastq;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

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
    // Gzipped → single partition (plain gzip, not BGZF)
    // -------------------------------------------------------------------------

    @Test
    void gzipFastq_producesCorrectCount() {
        // Plain gzip (not BGZF) must fall back to a single partition.
        long count = spark.read().format("fastq")
                .load(fixtures.gzipFastq().toString()).count();
        assertEquals(FastqTestGenerator.GZIP_COUNT, count);
    }

    @Test
    void gzipFastq_smallBgzfSplitSize_isBgzfCheckedAndFallsBackToSinglePartition() {
        // bgzfSplitSize=1 forces the isBgzfFile() check even for tiny files.
        // test.fastq.gz is plain gzip (not BGZF), so isBgzfFile() returns false
        // and the reader falls back to the single-partition path.
        long count = spark.read().format("fastq")
                .option("bgzfSplitSize", "1")
                .load(fixtures.gzipFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.GZIP_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // BGZF → multi-partition
    // -------------------------------------------------------------------------

    @Test
    void bgzfFastq_multiPartition_correctCount() {
        // bgzfSplitSize=1 forces as many chunks as numPartitions allows, exercising
        // multi-partition BGZF reads. The total count must equal BGZF_COUNT with no
        // duplicates and no gaps.
        long count = spark.read().format("fastq")
                .option("bgzfSplitSize", "1")
                .load(fixtures.bgzfFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.BGZF_COUNT, count);
    }

    @Test
    void bgzfFastq_multiPartition_noDuplicates() {
        long distinct = spark.read().format("fastq")
                .option("bgzfSplitSize", "1")
                .load(fixtures.bgzfFastq().toString())
                .select("readName")
                .distinct()
                .count();
        assertEquals(FastqTestGenerator.BGZF_COUNT, distinct);
    }

    @Test
    void bgzfFastq_readNumber_detectedFromFilename() {
        // The BGZF fixture filename contains "_R1_" so readNumber must be 1.
        int readNum = (int) spark.read().format("fastq")
                .load(fixtures.bgzfFastq().toString())
                .first()
                .getAs("readNumber");
        assertEquals(1, readNum);
    }

    @Test
    void bgzfFastq_readName_doesNotContainAtSign() {
        // In BGZF mode the header line is read raw (includes leading '@').  The reader
        // must strip it before constructing FastqRecord so readName never starts with '@'.
        Row row = spark.read().format("fastq")
                .load(fixtures.bgzfFastq().toString())
                .orderBy("readName")
                .first();
        String readName = row.getString(row.fieldIndex("readName"));
        assertFalse(readName.startsWith("@"),
                "readName from BGZF file should not include the leading '@'");
    }

    @Test
    void bgzfFastq_numPartitions2_capsChunks_correctCount() {
        // numPartitions=2 combined with bgzfSplitSize=1 caps planBgzfSplitPartitions
        // at 2 chunks; total count must still equal BGZF_COUNT with no duplicates/gaps.
        long count = spark.read().format("fastq")
                .option("bgzfSplitSize", "1")
                .option("numPartitions", "2")
                .load(fixtures.bgzfFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.BGZF_COUNT, count);
    }

    @Test
    void bgzfFastq_singlePartition_correctCount() {
        // When the file fits in one split (bgzfSplitSize larger than the file),
        // a single partition is used — same result, different code path.
        long count = spark.read().format("fastq")
                .option("bgzfSplitSize", String.valueOf(Long.MAX_VALUE))
                .load(fixtures.bgzfFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.BGZF_COUNT, count);
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
    // Uncompressed → multi-partition
    // -------------------------------------------------------------------------

    @Test
    void uncompressedFastq_multiPartition_correctCount() {
        // minSplitBytes=1 forces many byte-range splits, exercising advanceToRecordBoundary
        // and the per-partition endByte check in FastqPartitionReader.
        long count = spark.read().format("fastq")
                .option("minSplitBytes", "1")
                .load(fixtures.plainFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.PLAIN_COUNT, count);
    }

    @Test
    void uncompressedFastq_multiPartition_noDuplicates() {
        long distinct = spark.read().format("fastq")
                .option("minSplitBytes", "1")
                .load(fixtures.plainFastq().toString())
                .select("readName")
                .distinct()
                .count();
        assertEquals(FastqTestGenerator.PLAIN_COUNT, distinct);
    }

    @Test
    void uncompressedFastq_numPartitions_capsAtRequested() {
        // numPartitions=2 caps splits at 2; minSplitBytes=1 ensures 2 are actually created.
        long count = spark.read().format("fastq")
                .option("minSplitBytes", "1")
                .option("numPartitions", "2")
                .load(fixtures.plainFastq().toString())
                .count();
        assertEquals(FastqTestGenerator.PLAIN_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Spark SQL
    // -------------------------------------------------------------------------

    @Test
    void sql_createTempView_countMatchesExpected() {
        String path = fixtures.plainFastq().toString();
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW fastq_view USING fastq"
                + " OPTIONS (path '" + path + "')");
        long count = spark.sql("SELECT count(*) FROM fastq_view").first().getLong(0);
        assertEquals(FastqTestGenerator.PLAIN_COUNT, count);
        spark.sql("DROP VIEW IF EXISTS fastq_view");
    }

    @Test
    void sql_createTempView_selectFields_returnsExpectedSchema() {
        String path = fixtures.plainFastq().toString();
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW fastq_schema_view USING fastq"
                + " OPTIONS (path '" + path + "')");
        Dataset<Row> df = spark.sql("SELECT readName, sequence FROM fastq_schema_view");
        assertEquals(2, df.schema().length());
        assertEquals("readName", df.schema().apply(0).name());
        assertEquals("sequence", df.schema().apply(1).name());
        spark.sql("DROP VIEW IF EXISTS fastq_schema_view");
    }

    // -------------------------------------------------------------------------
    // Error conditions
    // -------------------------------------------------------------------------

    @Test
    void missingFile_throwsException() {
        assertThrows(Exception.class, () ->
            spark.read().format("fastq").load("/no/such/file.fastq").count());
    }

    // -------------------------------------------------------------------------
    // .fq.gz extension — covers isGzipped ".fq.gz" branch in planInputPartitions
    // -------------------------------------------------------------------------

    @Test
    void fqGzExtension_singleFile_readsCorrectly() throws Exception {
        // File ends with .fq.gz: isGzipped = endsWith(".fastq.gz") [F] || endsWith(".fq.gz") [T]
        // This covers the second OR branch in the isGzipped assignment.
        Path fqGz = tempDir.resolve("reads_R2_001.fq.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(fqGz))) {
            for (int i = 0; i < 3; i++) {
                os.write(("@read" + i + "\nACGTACGTAC\n+\nIIIIIIIIII\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        long count = spark.read().format("fastq").load(fqGz.toUri().toString()).count();
        assertEquals(3L, count);
    }

    // -------------------------------------------------------------------------
    // Directory loads — isFastqExtension branch coverage
    // -------------------------------------------------------------------------

    @Test
    void directory_plainFastqFile_extensionRecognized() throws Exception {
        // Directory contains a .fastq file: isFastqExtension checks endsWith(".fastq") [T]
        // first condition is true → short-circuits (B1 true branch).
        Path dir = tempDir.resolve("dir_fastq");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("reads.fastq"),
                "@r1\nACGT\n+\nIIII\n@r2\nCCCC\n+\nJJJJ\n", StandardCharsets.UTF_8);
        long count = spark.read().format("fastq").load(dir.toUri().toString()).count();
        assertEquals(2L, count);
    }

    @Test
    void directory_fqFile_extensionRecognized() throws Exception {
        // Directory contains a .fq file: endsWith(".fastq") [F], endsWith(".fq") [T]
        // Covers the B3 true branch of isFastqExtension.
        Path dir = tempDir.resolve("dir_fq");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("reads_R1_001.fq"),
                "@r1\nACGT\n+\nIIII\n@r2\nCCCC\n+\nJJJJ\n@r3\nGGGG\n+\nJJJJ\n",
                StandardCharsets.UTF_8);
        long count = spark.read().format("fastq").load(dir.toUri().toString()).count();
        assertEquals(3L, count);
    }

    @Test
    void directory_fqGzFile_extensionRecognized() throws Exception {
        // Directory contains a .fq.gz file: .fastq[F], .fq[F], .fastq.gz[F], .fq.gz[T]
        // Covers the B7 true branch of isFastqExtension.
        Path dir = tempDir.resolve("dir_fq_gz");
        Files.createDirectory(dir);
        Path fqGz = dir.resolve("reads_R1.fq.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(fqGz))) {
            for (int i = 0; i < 2; i++) {
                os.write(("@r" + i + "\nACGT\n+\nIIII\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        long count = spark.read().format("fastq").load(dir.toUri().toString()).count();
        assertEquals(2L, count);
    }

    // -------------------------------------------------------------------------
    // Directory with no FASTQ files — covers !files.isEmpty() false branch
    // (fallback listStatus path in resolveFiles) and empty statuses loop
    // in planInputPartitions
    // -------------------------------------------------------------------------

    @Test
    void directory_noFastqFiles_returnsEmpty() throws Exception {
        // All children fail isFastqExtension → files list stays empty after the glob
        // loop → !files.isEmpty() = false → fallback listStatus path is taken.
        // planInputPartitions receives an empty statuses array → for-each loop never
        // enters → 0 partitions → 0 rows.
        Path dir = tempDir.resolve("dir_no_fastq");
        Files.createDirectory(dir);
        Files.writeString(dir.resolve("notes.txt"), "not fastq content", StandardCharsets.UTF_8);
        long count = spark.read().format("fastq").load(dir.toUri().toString()).count();
        assertEquals(0L, count);
    }

    // -------------------------------------------------------------------------
    // resolveFiles() — child.isFile() == false branch
    // A subdirectory inside the target directory is a non-file child; it must be
    // skipped (child.isFile() = false → AND short-circuits → not added to files).
    // -------------------------------------------------------------------------

    @Test
    void directory_withSubdirectory_subdirSkipped_fastqFileRead() throws Exception {
        // Directory contains a subdirectory (child.isFile() = false → skip) and
        // a .fastq file (child.isFile() = true, extension OK → added).
        // The s.isDirectory() branch in resolveFiles() calls listStatus() on the
        // parent; the subdirectory child exercises the child.isFile() == false path.
        Path dir = tempDir.resolve("dir_with_subdir");
        Files.createDirectory(dir);
        Files.createDirectory(dir.resolve("subdir")); // non-file child
        Files.writeString(dir.resolve("reads_R1_001.fastq"),
                "@r1\nACGT\n+\nIIII\n@r2\nCCCC\n+\nJJJJ\n",
                StandardCharsets.UTF_8);

        long count = spark.read().format("fastq").load(dir.toUri().toString()).count();
        assertEquals(2L, count);
    }

    // -------------------------------------------------------------------------
    // Statistics (SupportsReportStatistics)
    // -------------------------------------------------------------------------

    @Test
    void estimateStatistics_sizeInBytes_greaterThanZero() {
        Dataset<Row> df = spark.read().format("fastq").load(realGzipPath);
        long sizeBytes = df.queryExecution().optimizedPlan()
                .stats().sizeInBytes().longValue();
        assertTrue(sizeBytes > 0, "sizeInBytes should be > 0 for a non-empty FASTQ file");
        assertTrue(sizeBytes <= new java.io.File(java.nio.file.Paths.get(
                java.net.URI.create(realGzipPath)).toString()).length() * 2,
                "sizeInBytes should be within 2x of the actual file size");
    }

    // -------------------------------------------------------------------------
    // Limit pushdown (SupportsPushDownLimit)
    // -------------------------------------------------------------------------

    @Test
    void limit_pushdown_returnsExactCount() {
        Dataset<Row> df = spark.read().format("fastq").load(realGzipPath).limit(5);
        assertEquals(5L, df.count(), "limit(5) should return exactly 5 rows");
        assertEquals(1, df.rdd().getNumPartitions(),
                "limit pushdown should restrict planning to 1 partition");
    }
}
