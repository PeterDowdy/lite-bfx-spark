package io.github.peterdowdy.litebfx.bam;

import htsjdk.samtools.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;

/**
 * Generates deterministic BAM, BAI, and SAM test fixtures using htsjdk.
 *
 * <p>Produces {@value #RECORD_COUNT} coordinate-sorted reads on a single reference
 * sequence ({@value #REF_NAME}), positioned at multiples of 100 starting at 100.
 * All reads are single-end, mapped, forward strand, 50 bp, with {@code NM:i:0}.
 *
 * <p>The expected SAM content (verified by opening the generated files in samtools
 * or any SAM-compatible tool) is:
 * <pre>
 * @HD VN:1.6 SO:coordinate
 * @SQ SN:chr1 LN:1000000
 * read1   0   chr1   100  60  50M  *  0  0  AAAA...  IIII...  NM:i:0
 * read2   0   chr1   200  60  50M  *  0  0  AAAA...  IIII...  NM:i:0
 * ...
 * read10  0   chr1  1000  60  50M  *  0  0  AAAA...  IIII...  NM:i:0
 * </pre>
 */
public class TestBamGenerator {

    public static final String REF_NAME = "chr1";
    public static final int REF_LENGTH = 1_000_000;
    public static final int RECORD_COUNT = 10;
    public static final int MAPPING_QUALITY = 60;
    public static final String CIGAR = "50M";
    public static final String SEQUENCE = "A".repeat(50);
    /** ASCII Phred+33: 'I' = 73, Phred score = 40 (maximum quality). */
    public static final String BASE_QUALITIES = "I".repeat(50);

    /** Paths to the generated files. */
    public record Fixtures(Path bam, Path bai, Path sam) {}

    /**
     * Generates test.bam + test.bam.bai + test.sam inside {@code dir}.
     *
     * @param dir directory in which to write files (must exist and be writable)
     * @return paths to the generated files
     */
    public static Fixtures generate(Path dir) throws IOException {
        SAMFileHeader header = buildHeader();
        Path bamPath = dir.resolve("test.bam");
        Path samPath = dir.resolve("test.sam");
        Path baiPath = dir.resolve("test.bam.bai");

        writeBam(header, bamPath.toFile());
        writeSam(header, samPath.toFile());

        // htsjdk may write the index as test.bai instead of test.bam.bai; normalise here.
        Path altBaiPath = dir.resolve("test.bai");
        if (!baiPath.toFile().exists() && altBaiPath.toFile().exists()) {
            Files.move(altBaiPath, baiPath);
        }

        return new Fixtures(bamPath, baiPath, samPath);
    }

    private static SAMFileHeader buildHeader() {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(REF_NAME, REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        return header;
    }

    private static void writeBam(SAMFileHeader header, File bamFile) throws IOException {
        SAMFileWriterFactory factory = new SAMFileWriterFactory()
            .setCreateIndex(true);
        try (SAMFileWriter writer = factory.makeBAMWriter(header, true, bamFile)) {
            for (SAMRecord r : buildRecords(header)) {
                writer.addAlignment(r);
            }
        }
    }

    private static void writeSam(SAMFileHeader header, File samFile) throws IOException {
        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .makeSAMWriter(header, true, samFile)) {
            for (SAMRecord r : buildRecords(header)) {
                writer.addAlignment(r);
            }
        }
    }

    /**
     * Generates a BAM file with a single record whose read sequence is {@code seqLength}
     * all-A bases with all-I (Phred 40) quality scores.
     *
     * <p>Using uniform bases and quality scores makes the probe heuristic in
     * {@code findCleanRecordStart} reliable: every continuation block starts with
     * either packed-A sequence bytes ({@code 0x11111111} ≈ 286 M) or all-I quality
     * bytes ({@code 0x49494949} ≈ 1.23 B), both exceeding {@code MAX_BAM_RECORD_BODY}
     * (100 M), so false positives are impossible for these fixtures.
     *
     * <p>An 80 000 bp read produces ~120 kB of uncompressed record data → 2 data blocks
     * + 1 EOF block = 3 BGZF blocks total.  A 200 000 bp read produces ~300 kB →
     * 4 data blocks + 1 EOF block = 5 BGZF blocks total.
     */
    public static java.nio.file.Path generateLargeReadBam(java.nio.file.Path dir, int seqLength)
            throws IOException {
        SAMFileHeader header = buildHeader();
        java.nio.file.Path bamPath = dir.resolve("large_" + seqLength + ".bam");
        String seq  = "A".repeat(seqLength);
        String qual = "I".repeat(seqLength);

        SAMRecord r = new SAMRecord(header);
        r.setReadName("largeread");
        r.setFlags(0);
        r.setReferenceIndex(0);
        r.setAlignmentStart(100);
        r.setMappingQuality(MAPPING_QUALITY);
        r.setCigarString(seqLength + "M");
        r.setReadString(seq);
        r.setBaseQualityString(qual);
        r.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
        r.setMateAlignmentStart(0);
        r.setInferredInsertSize(0);

        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .setCreateIndex(false)
                .makeBAMWriter(header, true, bamPath.toFile())) {
            writer.addAlignment(r);
        }
        return bamPath;
    }

    /**
     * Generates a queryname-sorted BAM (no BAI) with {@value #RECORD_COUNT} records.
     * Used to verify that {@code BamScan.outputOrdering()} returns empty for non-coordinate sort.
     */
    public static Path generateQuerynameSortedBam(Path dir) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(REF_NAME, REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.queryname);
        Path bamPath = dir.resolve("queryname.bam");
        // presorted=false: records are in coordinate order; htsjdk sorts by queryname before writing
        try (SAMFileWriter writer = new SAMFileWriterFactory()
                .setCreateIndex(false)
                .makeBAMWriter(header, false, bamPath.toFile())) {
            for (SAMRecord r : buildRecords(header)) {
                writer.addAlignment(r);
            }
        }
        return bamPath;
    }

    private static SAMRecord[] buildRecords(SAMFileHeader header) {
        SAMRecord[] records = new SAMRecord[RECORD_COUNT];
        for (int i = 0; i < RECORD_COUNT; i++) {
            SAMRecord r = new SAMRecord(header);
            r.setReadName("read" + (i + 1));
            r.setFlags(0);                                              // mapped, forward, primary
            r.setReferenceIndex(0);                                     // chr1
            r.setAlignmentStart((i + 1) * 100);                        // 100, 200, ..., 1000
            r.setMappingQuality(MAPPING_QUALITY);
            r.setCigarString(CIGAR);
            r.setReadString(SEQUENCE);
            r.setBaseQualityString(BASE_QUALITIES);
            r.setMateReferenceIndex(SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX);
            r.setMateAlignmentStart(0);
            r.setInferredInsertSize(0);
            r.setAttribute("NM", 0);
            records[i] = r;
        }
        return records;
    }
}
