package com.litebfx.bam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Unit tests for {@link BamPartitionReader}.
 *
 * <p>All tests run locally without a SparkSession.  The test fixtures are
 * generated programmatically by {@link TestBamGenerator} so there are no
 * external file dependencies.  The generated BAM + SAM are equivalent to the
 * following samtools commands:
 * <pre>
 *   samtools view -c test.bam    # → 10
 *   samtools view    test.bam    # 10 lines; first line starts with "read1\t0\tchr1\t100\t60\t50M\t*\t0\t0"
 * </pre>
 */
class BamPartitionReaderTest {

    @TempDir
    static Path tempDir;

    static TestBamGenerator.Fixtures fixtures;

    @BeforeAll
    static void generateFixtures() throws IOException {
        fixtures = TestBamGenerator.generate(tempDir);
    }

    // -------------------------------------------------------------------------
    // BamSchema
    // -------------------------------------------------------------------------

    @Test
    void schema_hasExpectedFields() {
        var schema = BamSchema.SCHEMA;
        assertEquals(12, schema.length());
        assertEquals("readName",          schema.apply(0).name());
        assertEquals("flags",             schema.apply(1).name());
        assertEquals("referenceName",     schema.apply(2).name());
        assertEquals("start",             schema.apply(3).name());
        assertEquals("mappingQuality",    schema.apply(4).name());
        assertEquals("cigar",             schema.apply(5).name());
        assertEquals("mateReferenceName", schema.apply(6).name());
        assertEquals("mateStart",         schema.apply(7).name());
        assertEquals("insertSize",        schema.apply(8).name());
        assertEquals("sequence",          schema.apply(9).name());
        assertEquals("baseQualities",     schema.apply(10).name());
        assertEquals("attributes",        schema.apply(11).name());
    }

    // -------------------------------------------------------------------------
    // BAM reading
    // -------------------------------------------------------------------------

    @Test
    void bam_readAllRecords_countMatchesExpected() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), true);
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size());
    }

    @Test
    void bam_firstRecord_scalarFieldsMatchExpected() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), true);
        InternalRow row = rows.get(0);

        assertEquals("read1",                 row.getUTF8String(0).toString()); // readName
        assertEquals(0,                        row.getInt(1));                   // flags
        assertEquals(TestBamGenerator.REF_NAME, row.getUTF8String(2).toString()); // referenceName
        assertEquals(100,                      row.getInt(3));                   // start (1-based)
        assertEquals(TestBamGenerator.MAPPING_QUALITY, row.getInt(4));           // mappingQuality
        assertEquals(TestBamGenerator.CIGAR,   row.getUTF8String(5).toString()); // cigar
        assertNull(                            row.getUTF8String(6));            // mateReferenceName (unmapped)
        assertEquals(0,                        row.getInt(7));                   // mateStart
        assertEquals(0,                        row.getInt(8));                   // insertSize
        assertEquals(TestBamGenerator.SEQUENCE,      row.getUTF8String(9).toString());  // sequence
        assertEquals(TestBamGenerator.BASE_QUALITIES, row.getUTF8String(10).toString()); // baseQualities
    }

    @Test
    void bam_recordPositionsAreMonotonicallyIncreasing() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), false);
        for (int i = 0; i < rows.size(); i++) {
            assertEquals((i + 1) * 100, rows.get(i).getInt(3),
                "start position for read" + (i + 1));
        }
    }

    @Test
    void bam_readNamesMatchExpected() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), false);
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("read" + (i + 1), rows.get(i).getUTF8String(0).toString());
        }
    }

    @Test
    void bam_attributesMapContainsNm() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), true);
        InternalRow row = rows.get(0);

        assertFalse(row.isNullAt(11), "attributes should not be null");
        MapData attrs = row.getMap(11);
        Map<String, String> map = toJavaMap(attrs);
        assertTrue(map.containsKey("NM"), "attributes should contain NM tag");
        assertEquals("0", map.get("NM"));
    }

    @Test
    void bam_attributesOmittedWhenNotRequired() throws IOException {
        List<InternalRow> rows = readAll(fixtures.bam(), false);
        InternalRow row = rows.get(0);
        assertTrue(row.isNullAt(11), "attributes should be null when includeAttributes=false");
    }

    // -------------------------------------------------------------------------
    // SAM reading (same data, no index)
    // -------------------------------------------------------------------------

    @Test
    void sam_readAllRecords_countMatchesBam() throws IOException {
        List<InternalRow> samRows = readAll(fixtures.sam(), false);
        assertEquals(TestBamGenerator.RECORD_COUNT, samRows.size());
    }

    @Test
    void sam_firstRecord_scalarFieldsMatchExpected() throws IOException {
        List<InternalRow> rows = readAll(fixtures.sam(), true);
        InternalRow row = rows.get(0);

        assertEquals("read1", row.getUTF8String(0).toString());
        assertEquals(0,       row.getInt(1));
        assertEquals(TestBamGenerator.REF_NAME, row.getUTF8String(2).toString());
        assertEquals(100,     row.getInt(3));
        assertEquals(TestBamGenerator.CIGAR, row.getUTF8String(5).toString());
        assertEquals(TestBamGenerator.SEQUENCE, row.getUTF8String(9).toString());
        assertEquals(TestBamGenerator.BASE_QUALITIES, row.getUTF8String(10).toString());
    }

    // -------------------------------------------------------------------------
    // VFO-based per-reference partitioning
    // -------------------------------------------------------------------------

    /**
     * Verifies that a per-reference partition (querySequences set, no BAI available
     * in this unit-test context) falls back to a full scan and still reads all records.
     * With BAI, htsjdk uses the BAI VFOs internally; without BAI, it scans the file.
     */
    @Test
    void perRefPartition_singleRef_readsAllRecords() throws IOException {
        BamInputPartition partition = BamInputPartition.forVfoPartitions(
            fixtures.bam().toUri().toString(), new Configuration(),
            /* indexPath */ null, null, "none",
            new String[]{TestBamGenerator.REF_NAME});

        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size(),
            "single-ref partition should return all records for that reference");
    }

    @Test
    void perRefPartition_withBaiIndex_readsAllRecords() throws IOException {
        String baiUri = fixtures.bai().toUri().toString();
        BamInputPartition partition = BamInputPartition.forVfoPartitions(
            fixtures.bam().toUri().toString(), new Configuration(),
            /* indexPath */ baiUri, null, "none",
            new String[]{TestBamGenerator.REF_NAME});

        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size(),
            "single-ref BAI-guided partition should return all records for that reference");
    }

    @Test
    void unmappedPartition_withBaiIndex_returnsNoRecordsForAllMappedFile() throws IOException {
        // The synthetic BAM has no unmapped reads; queryUnmapped() should return 0 rows.
        String baiUri = fixtures.bai().toUri().toString();
        BamInputPartition partition = BamInputPartition.forUnmapped(
            fixtures.bam().toUri().toString(), new Configuration(),
            /* indexPath */ baiUri, null, "none");

        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        assertEquals(0, rows.size(),
            "unmapped partition should return 0 records when all reads are mapped");
    }

    // -------------------------------------------------------------------------
    // BGZF split mode
    // -------------------------------------------------------------------------

    /**
     * Splits exactly at the EOF BGZF block boundary (aligned split), and verifies
     * that no records are lost or duplicated.
     *
     * <p>The small 10-record BAM is written by htsjdk into a single compressed data
     * block followed by the BGZF EOF block.  Splitting at the start of the EOF block
     * means chunk 0 owns the entire data block (all 10 records) and chunk 1 starts
     * at the EOF block, finds no valid record-body length in its 0-byte decompressed
     * content, and yields 0 records.  This verifies that:
     * <ul>
     *   <li>the block-address stop condition fires correctly at an aligned boundary, and</li>
     *   <li>a partition that starts at the EOF block is correctly empty (no double-counting).</li>
     * </ul>
     */
    @Test
    void bgzfSplit_aligned_splitAtEofBlock_allRecordsInFirstChunk() throws IOException {
        List<Long> blockStarts = findBgzfBlockStarts(fixtures.bam());
        assumeTrue(blockStarts.size() >= 2, "need at least one data block + EOF block");
        // Last block is always the BGZF EOF sentinel.
        long splitByte = blockStarts.get(blockStarts.size() - 1);

        List<InternalRow> chunk0 = readPartition(fixtures.bam(), 0L, splitByte);
        List<InternalRow> chunk1 = readPartition(fixtures.bam(), splitByte, Long.MAX_VALUE);

        assertEquals(TestBamGenerator.RECORD_COUNT, chunk0.size(),
            "all data precedes the EOF block — chunk0 owns all records");
        assertEquals(0, chunk1.size(),
            "chunk1 starts at the EOF block — probe finds 0 decompressed bytes, no valid start");
    }

    /**
     * One record spanning two chunks.
     *
     * <p>A single read with an 80 000 bp sequence produces a BAM record body
     * ≈ 105 000 bytes uncompressed, spanning at least two BGZF blocks.
     * The chunk boundary is placed inside the first data block (between
     * blockStarts[1] and blockStarts[2]), guaranteeing the boundary is mid-record.
     * Only chunk 0 (which contains the block where the record starts) may yield
     * the record; chunk 1 finds only continuation blocks, whose first 4 decompressed
     * bytes are not a valid record-body length, so it yields 0 records.
     */
    @Test
    void bgzfSplit_recordSpansTwoChunks_firstChunkOwnsRecord() throws IOException {
        Path largeBam = TestBamGenerator.generateLargeReadBam(tempDir, 80_000);
        List<Long> blockStarts = findBgzfBlockStarts(largeBam);
        assumeTrue(blockStarts.size() >= 3,
            "need header block + ≥1 data block + EOF for 80 000 bp record");

        // Split inside block 1 (mid-record): boundary is between blockStarts[1] and blockStarts[2].
        long splitByte = (blockStarts.get(1) + blockStarts.get(2)) / 2;

        List<InternalRow> chunk0 = readPartition(largeBam, 0L, splitByte);
        List<InternalRow> chunk1 = readPartition(largeBam, splitByte, Long.MAX_VALUE);

        assertEquals(1, chunk0.size() + chunk1.size(),
            "union of two chunks must yield exactly 1 record");
        assertEquals(1, chunk0.size(),
            "chunk0 owns the record — its start block (block 1) precedes splitByte");
        assertEquals(0, chunk1.size(),
            "chunk1 starts mid-record — findCleanRecordStart finds no valid block start");
    }

    /**
     * One record spanning three chunks.
     *
     * <p>A 200 000 bp sequence produces a record body ≈ 300 000 bytes uncompressed,
     * spanning ≥ 4 BGZF data blocks.  The two chunk boundaries are placed inside
     * blocks 1 and 2 respectively (both mid-record).  Only chunk 0 yields the record;
     * chunks 1 and 2 each find only continuation/tail blocks and yield 0 records.
     * This verifies that a partition whose entire range is covered by a single
     * cross-block record correctly yields nothing without corrupting data.
     */
    @Test
    void bgzfSplit_recordSpansThreeChunks_onlyFirstChunkOwnsRecord() throws IOException {
        Path largeBam = TestBamGenerator.generateLargeReadBam(tempDir, 200_000);
        List<Long> blockStarts = findBgzfBlockStarts(largeBam);
        assumeTrue(blockStarts.size() >= 4,
            "need header block + ≥2 data blocks for split points + EOF for 200 000 bp record");

        // Both boundaries inside record data blocks — chunk 1 is entirely mid-record.
        long split1 = (blockStarts.get(1) + blockStarts.get(2)) / 2;
        long split2 = (blockStarts.get(2) + blockStarts.get(3)) / 2;

        List<InternalRow> chunk0 = readPartition(largeBam, 0L,     split1);
        List<InternalRow> chunk1 = readPartition(largeBam, split1, split2);
        List<InternalRow> chunk2 = readPartition(largeBam, split2, Long.MAX_VALUE);

        assertEquals(1, chunk0.size() + chunk1.size() + chunk2.size(),
            "union of 3 chunks must yield exactly 1 record");
        assertEquals(1, chunk0.size(),
            "chunk0 owns the record — record starts in block 1, before split1");
        assertEquals(0, chunk1.size(),
            "chunk1 is entirely mid-record (continuation blocks only) — no clean start found");
        assertEquals(0, chunk2.size(),
            "chunk2 covers the record tail — no clean start found (tail blocks rejected by probe)");
    }

    // -------------------------------------------------------------------------
    // SAM line-split mode
    // -------------------------------------------------------------------------

    @Test
    void samSplit_singleChunk_matchesFullScan() throws IOException {
        List<InternalRow> rows = readSamPartition(fixtures.sam(), 0L, Long.MAX_VALUE);
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size(),
            "single SAM chunk (0 → MAX) should return all records");
    }

    /**
     * Three roughly equal byte-range partitions over the generated SAM file
     * (boundaries at ~33 % and ~66 % of file size).
     *
     * <p>Each worker must return at least one record, proving that lines are
     * distributed across all three partitions and no records are lost or duplicated.
     */
    @Test
    void samSplit_threeWorkers_eachGetsMultipleRecords() throws IOException {
        long fileSize = Files.size(fixtures.sam());
        long split1 = fileSize / 3;
        long split2 = (fileSize / 3) * 2;

        List<InternalRow> chunk0 = readSamPartition(fixtures.sam(), 0L,     split1);
        List<InternalRow> chunk1 = readSamPartition(fixtures.sam(), split1, split2);
        List<InternalRow> chunk2 = readSamPartition(fixtures.sam(), split2, Long.MAX_VALUE);

        int total = chunk0.size() + chunk1.size() + chunk2.size();
        assertEquals(TestBamGenerator.RECORD_COUNT, total,
            "union of 3 SAM chunks must yield exactly " + TestBamGenerator.RECORD_COUNT + " records");
        assertTrue(chunk0.size() > 0, "chunk0 (0 → 33%) should contain at least one record");
        assertTrue(chunk1.size() > 0, "chunk1 (33% → 66%) should contain at least one record");
        assertTrue(chunk2.size() > 0, "chunk2 (66% → EOF) should contain at least one record");

        // No duplicates: all read names across all chunks are distinct.
        java.util.Set<String> names = new java.util.HashSet<>();
        for (List<InternalRow> chunk : java.util.List.of(chunk0, chunk1, chunk2)) {
            for (InternalRow row : chunk) {
                assertTrue(names.add(row.getUTF8String(0).toString()),
                    "duplicate readName detected — record counted twice");
            }
        }
    }

    /**
     * Splits the SAM into 1-byte chunks (one per byte of the file) and unions all
     * partitions. The total record count must equal the full-scan count, and every
     * expected read name must appear exactly once.
     */
    @Test
    void samSplit_multipleChunks_noLossNoDuplication() throws IOException {
        long fileSize = Files.size(fixtures.sam());
        long splitSize = 1L;
        int numChunks = (int) Math.ceil((double) fileSize / splitSize);

        List<InternalRow> all = new ArrayList<>();
        for (int i = 0; i < numChunks; i++) {
            long start = (long) i * splitSize;
            long end   = (i == numChunks - 1) ? Long.MAX_VALUE : (long) (i + 1) * splitSize;
            all.addAll(readSamPartition(fixtures.sam(), start, end));
        }

        assertEquals(TestBamGenerator.RECORD_COUNT, all.size(),
            "union of 1-byte SAM chunks must yield exactly " + TestBamGenerator.RECORD_COUNT + " records");

        java.util.Set<String> names = new java.util.HashSet<>();
        for (InternalRow row : all) {
            assertTrue(names.add(row.getUTF8String(0).toString()),
                "duplicate readName detected — record counted twice");
        }
        assertEquals(TestBamGenerator.RECORD_COUNT, names.size(),
            "all expected read names must be present");
    }

    @Test
    void samSplit_chunkPastEndOfFile_isEmpty() throws IOException {
        long fileSize = Files.size(fixtures.sam());
        List<InternalRow> rows = readSamPartition(fixtures.sam(), fileSize + 1, Long.MAX_VALUE);
        assertEquals(0, rows.size(), "partition starting past EOF should be empty");
    }

    @Test
    void samSplit_headerOnlyFile_returnsNoRecords() throws IOException {
        // SAM with only @HD + @SQ lines and no data records.
        // openSamSplit() reads until line == null → dataStartByte = lineEndPos (EOF).
        // Partition 0 seeks to dataStartByte which equals fileLength → empty.
        Path headerOnly = tempDir.resolve("headeronly.sam");
        Files.writeString(headerOnly,
                "@HD\tVN:1.6\tSO:coordinate\n" +
                "@SQ\tSN:chr1\tLN:1000000\n");
        List<InternalRow> rows = readSamPartition(headerOnly, 0L, Long.MAX_VALUE);
        assertEquals(0, rows.size(), "header-only SAM must yield no records");
    }

    @Test
    void samSplit_positionPastEndByteAfterSkip_emptyPartition() throws IOException {
        // Partition whose startByte falls mid-line in the last line of the file.
        // After discarding the partial line, position >= endByte → samLineParser stays null.
        long fileSize = Files.size(fixtures.sam());
        // Set endByte equal to startByte so there is no room for any line.
        long midLine = fileSize - 3;   // inside the last line
        List<InternalRow> rows = readSamPartition(fixtures.sam(), midLine, midLine);
        assertEquals(0, rows.size(),
            "partition where post-skip position >= endByte should be empty");
    }

    @Test
    void samSplit_blankLineInData_skipped() throws IOException {
        // SAM with a blank line between data records.
        // next() reads the blank line → line.isEmpty() TRUE → continue (skips it).
        Path sam = tempDir.resolve("blank_data.sam");
        Files.writeString(sam,
                "@HD\tVN:1.6\tSO:coordinate\n" +
                "@SQ\tSN:chr1\tLN:1000000\n" +
                "read1\t0\tchr1\t100\t60\t50M\t*\t0\t0\t" + "A".repeat(50) + "\t" + "I".repeat(50) + "\n" +
                "\n" +     // blank line
                "read2\t0\tchr1\t200\t60\t50M\t*\t0\t0\t" + "A".repeat(50) + "\t" + "I".repeat(50) + "\n");
        List<InternalRow> rows = readSamPartition(sam, 0L, Long.MAX_VALUE);
        assertEquals(2, rows.size(), "blank line in SAM data must be skipped");
        assertEquals("read1", rows.get(0).getUTF8String(0).toString());
        assertEquals("read2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void samSplit_noTrailingNewline_lastRecordReturned() throws IOException {
        // SAM file whose last data line has no trailing '\n'.
        // readLineFrom() reaches EOF with sb non-empty → sb.length() > 0 TRUE → returns string.
        Path sam = tempDir.resolve("no_trailing_newline.sam");
        String content =
                "@HD\tVN:1.6\tSO:coordinate\n" +
                "@SQ\tSN:chr1\tLN:1000000\n" +
                "read1\t0\tchr1\t100\t60\t50M\t*\t0\t0\t" + "A".repeat(50) + "\t" + "I".repeat(50);
        Files.write(sam, content.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        List<InternalRow> rows = readSamPartition(sam, 0L, Long.MAX_VALUE);
        assertEquals(1, rows.size(), "last record with no trailing newline must still be returned");
        assertEquals("read1", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void samSplit_crlfLineEndings_readCorrectly() throws IOException {
        // SAM with CRLF (\r\n) line endings.
        // readLineFrom(): b == '\r' → b != '\r' FALSE → character NOT appended (stripped).
        Path sam = tempDir.resolve("crlf.sam");
        String content =
                "@HD\tVN:1.6\tSO:coordinate\r\n" +
                "@SQ\tSN:chr1\tLN:1000000\r\n" +
                "read1\t0\tchr1\t100\t60\t50M\t*\t0\t0\t" + "A".repeat(50) + "\t" + "I".repeat(50) + "\r\n";
        Files.write(sam, content.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        List<InternalRow> rows = readSamPartition(sam, 0L, Long.MAX_VALUE);
        assertEquals(1, rows.size(), "CRLF SAM must parse correctly");
        assertEquals("read1", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // BGZF split — findNextBgzfBlockStart and findCleanRecordStart edge cases
    // -------------------------------------------------------------------------

    @Test
    void bgzf_startByte1_twoBlockBam_eofDuringBlockScan_readsAllRecords() throws IOException {
        // Two-block BAM (header in block 0, records in block 1): findNextBgzfBlockStart
        // scans from byte 1, hits EOF before filling its 65 KB buffer → n < 0 branch.
        // Block 1 is found within the buffer; all records are read until
        // bamRecordCodec.decode() returns null at EOF → r == null TRUE branch.
        // Also covers endByte != MAX_VALUE FALSE (endByte = MAX_VALUE, no early stop).
        Path twoBlock = createTwoBlockBam(tempDir.resolve("two_block.bam"));
        List<InternalRow> rows = readPartition(twoBlock, 1L, Long.MAX_VALUE);
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size(),
            "starting from byte 1 in a two-block BAM must still read all data records");
    }

    @Test
    void bgzf_startNearEof_lessThan4BytesRead_emptyPartition() throws IOException {
        // Start 2 bytes before EOF: findNextBgzfBlockStart reads 2 bytes (< 4),
        // the for-loop condition i <= 2-4 = -2 is false on the first check →
        // loop body never executes → returns -1 → empty partition.
        long fileSize = Files.size(fixtures.bam());
        List<InternalRow> rows = readPartition(fixtures.bam(), fileSize - 2, Long.MAX_VALUE);
        assertEquals(0, rows.size(), "only 2 bytes before EOF yields no BGZF block");
    }

    @Test
    void bgzf_startAtEofBlock_probeReturnsEof_emptyPartition() throws IOException {
        // Start exactly at the 28-byte empty EOF BGZF block.
        // findNextBgzfBlockStart finds the EOF block's magic at offset 0 in the scan.
        // findCleanRecordStart probes its decompressed content: bcis.read() returns -1
        // (0 bytes in EOF block) → n != 4 FALSE branch → advances to next block →
        // no next block → returns -1 → empty partition.
        long fileSize = Files.size(fixtures.bam());
        long eofBlockOffset = fileSize - 28;  // empty EOF BGZF block is always 28 bytes
        List<InternalRow> rows = readPartition(fixtures.bam(), eofBlockOffset, Long.MAX_VALUE);
        assertEquals(0, rows.size(), "partition starting at EOF block must be empty");
    }

    // -------------------------------------------------------------------------
    // open() — partition modes not yet covered
    // -------------------------------------------------------------------------

    @Test
    void open_queryUnmapped_noIndex_opensWithoutIndex() {
        // queryUnmapped=true with no index: open() takes the getIndexPath()==null branch
        // (opens the BAM without attaching a BAI stream). queryUnmapped() then throws
        // because a BAM without an index cannot seek to the unmapped section — but the
        // important thing is that the code reaches the else branch, which JaCoCo counts.
        BamInputPartition partition = BamInputPartition.forUnmapped(
                fixtures.bam().toUri().toString(), new Configuration(),
                /* indexPath */ null, null, "none");
        assertThrows(Exception.class, () -> readPartitionDirect(partition),
            "queryUnmapped without BAI must throw (but the no-index branch is still covered)");
    }

    @Test
    void open_querySequences_noIndex_fallsBackToFullScan() throws IOException {
        // querySequences set but indexPath == null → "no index; fall back to full-file scan"
        // branch taken in open(); samReader.iterator() returns all records.
        BamInputPartition partition = BamInputPartition.forVfoPartitions(
                fixtures.bam().toUri().toString(), new Configuration(),
                /* indexPath */ null, null, "none",
                new String[]{TestBamGenerator.REF_NAME});
        List<InternalRow> rows = readPartitionDirect(partition);
        assertEquals(TestBamGenerator.RECORD_COUNT, rows.size(),
            "no-index querySequences falls back to full scan and returns all records");
    }

    @Test
    void open_cramContainerSplit_emptySpans_returnsNoRecords() throws IOException {
        // cramContainerSpans non-null (→ CRAM container-split path in open()) but empty
        // (→ spans.length == 0 TRUE branch in openCramContainerSplit() → early return).
        BamInputPartition partition = BamInputPartition.forCramContainerSplit(
                fixtures.bam().toUri().toString(), new Configuration(),
                null, null, "none", /* cramContainerSpans */ new long[0]);
        List<InternalRow> rows = readPartitionDirect(partition);
        assertEquals(0, rows.size(), "empty CRAM spans must produce an empty partition");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Reads all rows from an already-constructed partition. */
    private static List<InternalRow> readPartitionDirect(BamInputPartition partition)
            throws IOException {
        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        return rows;
    }

    /** Opens a full-file partition for the given path and collects all rows. */
    private static List<InternalRow> readAll(Path path, boolean includeAttributes)
        throws IOException {
        BamInputPartition partition = BamInputPartition.forFullScan(
            path.toUri().toString(), new Configuration());

        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, includeAttributes)) {
            while (reader.next()) {
                rows.add(reader.get());
            }
        }
        return rows;
    }

    /** Converts {@link MapData} (StringType keys and values) to a Java Map. */
    private static Map<String, String> toJavaMap(MapData mapData) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < mapData.numElements(); i++) {
            UTF8String key   = mapData.keyArray().getUTF8String(i);
            UTF8String value = mapData.valueArray().getUTF8String(i);
            result.put(key.toString(), value.toString());
        }
        return result;
    }

    /**
     * Opens a BGZF split partition for the given path and byte range, and collects all rows.
     * Setting {@code endByte = Long.MAX_VALUE} makes this a single "open-ended" partition
     * that runs to EOF.
     */
    private static List<InternalRow> readPartition(Path bamPath, long startByte, long endByte)
            throws IOException {
        BamInputPartition partition = BamInputPartition.forBgzfSplit(
            bamPath.toUri().toString(), new Configuration(), startByte, endByte, null, "none");
        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        return rows;
    }

    /**
     * Opens a SAM line-split partition for the given path and byte range, and collects all rows.
     * Setting {@code endByte = Long.MAX_VALUE} makes this an open-ended partition that runs to EOF.
     */
    private static List<InternalRow> readSamPartition(Path samPath, long startByte, long endByte)
            throws IOException {
        BamInputPartition partition = BamInputPartition.forSamSplit(
            samPath.toUri().toString(), new Configuration(), startByte, endByte);
        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        return rows;
    }

    /**
     * Writes a BAM with RECORD_COUNT records in two separate BGZF data blocks:
     * the BAM header lands in block 0 (explicit flush after writing the header binary),
     * and all data records land in block 1.  This gives the file the structure:
     * [header BGZF block] [data BGZF block] [EOF BGZF block]
     * which is required for tests that need to scan from byte 1 and still find a
     * valid data block (rather than landing directly on the EOF block as would happen
     * with a single-block BAM).
     */
    private static Path createTwoBlockBam(Path dest) throws IOException {
        SAMFileHeader header = new SAMFileHeader();
        header.addSequence(new SAMSequenceRecord(TestBamGenerator.REF_NAME, TestBamGenerator.REF_LENGTH));
        header.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        try (BlockCompressedOutputStream bcos = new BlockCompressedOutputStream(dest.toFile())) {
            // Write the BAM header binary manually so we can flush the BGZF block
            // immediately after, forcing header and records into separate BGZF blocks.
            htsjdk.samtools.util.BinaryCodec bc = new htsjdk.samtools.util.BinaryCodec(bcos);
            // BAM magic
            bcos.write(new byte[]{'B', 'A', 'M', 1});
            // l_text + header text
            byte[] headerText = ("@HD\tVN:1.6\tSO:coordinate\n" +
                    "@SQ\tSN:" + TestBamGenerator.REF_NAME +
                    "\tLN:" + TestBamGenerator.REF_LENGTH + "\n")
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            bc.writeInt(headerText.length);
            bcos.write(headerText);
            // n_ref = 1 reference
            bc.writeInt(1);
            byte[] refName = (TestBamGenerator.REF_NAME + "\0").getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            bc.writeInt(refName.length);
            bcos.write(refName);
            bc.writeInt(TestBamGenerator.REF_LENGTH);
            bcos.flush(); // ← force header into its own BGZF block

            // Write records into the second BGZF block using a temporary BAM writer
            // approach: generate records via htsjdk's BAMRecordCodec.
            BAMRecordCodec codec = new BAMRecordCodec(header);
            codec.setOutputStream(bcos);
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
                r.setAttribute("NM", 0);
                codec.encode(r);
            }
        }
        return dest;
    }

    /**
     * Scans the raw bytes of {@code path} for the BGZF 4-byte magic
     * ({@code 0x1f 0x8b 0x08 0x04}) and returns the file offset of every match.
     * Used in tests to locate exact BGZF block boundaries so that chunk boundaries
     * can be placed precisely (aligned) or deliberately mid-block.
     */
    private static List<Long> findBgzfBlockStarts(Path path) throws IOException {
        byte[] data = Files.readAllBytes(path);
        List<Long> starts = new ArrayList<>();
        for (int i = 0; i <= data.length - 4; i++) {
            if ((data[i] & 0xff) == 0x1f && (data[i + 1] & 0xff) == 0x8b
                    && data[i + 2] == 0x08 && data[i + 3] == 0x04) {
                starts.add((long) i);
            }
        }
        return starts;
    }
}
