package com.litebfx.bam;

import htsjdk.samtools.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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

        writeBam(header, bamPath.toFile());
        writeSam(header, samPath.toFile());

        Path baiPath = dir.resolve("test.bam.bai");
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
