package com.litebfx.bam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link CramDataSource} via {@code spark.read.format("cram")}.
 *
 * <p>Uses a synthetic CRAM file backed by a generated FASTA reference.
 */
class CramDataSourceTest {

    static SparkSession spark;

    @TempDir
    static Path tempDir;

    static String cramPath;
    static String fastaPath;

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("CramDataSourceTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        Path faPath = generateFasta(tempDir);
        fastaPath = faPath.toAbsolutePath().toString();
        cramPath = generateCram(tempDir, faPath).toUri().toString();
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
        StructType schema = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .schema();
        assertEquals(BamSchema.SCHEMA, schema);
    }

    // -------------------------------------------------------------------------
    // Full-file count
    // -------------------------------------------------------------------------

    @Test
    void count_matchesRecordCount() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // referenceName filter (post-scan; all reads are on chr1)
    // -------------------------------------------------------------------------

    @Test
    void filter_referenceNameEquality_returnsCorrectCount() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .filter("referenceName = 'chr1'")
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Column pruning
    // -------------------------------------------------------------------------

    @Test
    void columnPruning_withoutAttributes_succeeds() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .load(cramPath)
                .select("readName", "start")
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // No-index fallback
    // -------------------------------------------------------------------------

    @Test
    void noIndexFallback_useIndexFalse_returnsAllRecords() {
        long count = spark.read().format("cram")
                .option("referenceFile", fastaPath)
                .option("useIndex", "false")
                .load(cramPath)
                .count();
        assertEquals(TestBamGenerator.RECORD_COUNT, count);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes a minimal FASTA reference for chr1 (all 'A' bases) and its .fai index.
     */
    static Path generateFasta(Path dir) throws IOException {
        Path faPath = dir.resolve("ref.fa");
        try (PrintWriter w = new PrintWriter(faPath.toFile())) {
            w.println(">" + TestBamGenerator.REF_NAME);
            String line = "A".repeat(70);
            int remaining = TestBamGenerator.REF_LENGTH;
            while (remaining > 0) {
                int len = Math.min(70, remaining);
                w.println("A".repeat(len));
                remaining -= len;
            }
        }
        FastaSequenceIndexCreator.create(faPath, true);
        return faPath;
    }

    /**
     * Writes a coordinate-sorted CRAM containing the same 10 synthetic records
     * as {@link TestBamGenerator}, backed by the provided FASTA reference.
     */
    static Path generateCram(Path dir, Path fastaRef) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(TestBamGenerator.REF_NAME, TestBamGenerator.REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        File cramFile = dir.resolve("test.cram").toFile();
        ReferenceSource refSource = new ReferenceSource(fastaRef.toFile());
        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .makeCRAMWriter(header, true, cramFile, fastaRef.toFile())) {
            for (SAMRecord r : buildRecords(header)) {
                writer.addAlignment(r);
            }
        }
        return cramFile.toPath();
    }

    private static SAMRecord[] buildRecords(SAMFileHeader header) {
        SAMRecord[] records = new SAMRecord[TestBamGenerator.RECORD_COUNT];
        for (int i = 0; i < TestBamGenerator.RECORD_COUNT; i++) {
            SAMRecord r = new SAMRecord(header);
            r.setReadName("read" + (i + 1));
            r.setFlags(0);
            r.setReferenceIndex(0);
            r.setAlignmentStart((i + 1) * 100);
            r.setMappingQuality(TestBamGenerator.MAPPING_QUALITY);
            r.setCigarString(TestBamGenerator.CIGAR);
            r.setReadString(TestBamGenerator.SEQUENCE);
            r.setBaseQualityString(TestBamGenerator.BASE_QUALITIES);
            r.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
            r.setMateAlignmentStart(0);
            r.setInferredInsertSize(0);
            r.setAttribute("NM", 0);
            records[i] = r;
        }
        return records;
    }
}
