package io.github.peterdowdy.litebfx.fastq;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FastqPartitionReader}.
 *
 * <p>Tests drive the reader directly (no SparkSession) using in-memory temp files.
 * Coverage targets: {@link FastqPartitionReader#get()} field parsing, plain full-scan mode,
 * gzip full-scan mode, plain-split mode ({@code nextPlainSplit} / {@code readLineRaw} /
 * {@code readPlainByte}), and {@code close()} before {@code next()}.
 */
class FastqPartitionReaderTest {

    @TempDir
    Path tempDir;

    static Configuration conf() {
        return new Configuration();
    }

    /** Read all rows from a partition into a list. */
    static List<InternalRow> readAll(FastqInputPartition p) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        try (FastqPartitionReader r = new FastqPartitionReader(p)) {
            while (r.next()) {
                rows.add(r.get());
            }
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // get() — field parsing
    // -------------------------------------------------------------------------

    @Test
    void get_readNameWithDescription_fieldsPopulated() throws Exception {
        Path file = tempDir.resolve("desc.fastq");
        Files.writeString(file, "@read1 some description here\nACGT\n+\nIIII\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertEquals("read1",                 r.getUTF8String(0).toString());  // readName
        assertEquals("ACGT",                  r.getUTF8String(1).toString());  // sequence
        assertEquals("IIII",                  r.getUTF8String(2).toString());  // baseQualities
        assertEquals("some description here", r.getUTF8String(3).toString()); // description
        assertTrue(r.isNullAt(4));                                              // readNumber null
    }

    @Test
    void get_readNameNoDescription_descriptionNull() throws Exception {
        Path file = tempDir.resolve("nodesc.fastq");
        Files.writeString(file, "@read2\nCCCC\n+\nJJJJ\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(3)); // description null when no space in header
    }

    @Test
    void get_readNameTrailingSpace_emptyDescriptionIsNull() throws Exception {
        // A trailing space with nothing after produces an empty description → null
        Path file = tempDir.resolve("trailspace.fastq");
        Files.writeString(file, "@read3 \nAAAA\n+\nIIII\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(3));
    }

    @Test
    void get_readNumber_encodedInRow() throws Exception {
        Path file = tempDir.resolve("r2.fastq");
        Files.writeString(file, "@read1\nAAAA\n+\nIIII\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf(), 2));

        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).getInt(4));
    }

    @Test
    void get_readNumber1_encodedInRow() throws Exception {
        Path file = tempDir.resolve("r1.fastq");
        Files.writeString(file, "@read1 d\nAAAA\n+\nIIII\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf(), 1));

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).getInt(4));
        assertEquals("d", rows.get(0).getUTF8String(3).toString());
    }

    // -------------------------------------------------------------------------
    // Plain full scan (uncompressed, startByte=0, endByte=MAX_VALUE)
    // -------------------------------------------------------------------------

    @Test
    void plainFullScan_readsAllRecords() throws Exception {
        Path file = tempDir.resolve("full.fastq");
        Files.writeString(file,
                "@r1\nACGT\n+\nIIII\n" +
                "@r2 desc\nCCCC\n+\nJJJJ\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(2, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("r2", rows.get(1).getUTF8String(0).toString());
        assertTrue(rows.get(0).isNullAt(3));          // r1 has no description
        assertEquals("desc", rows.get(1).getUTF8String(3).toString());
    }

    @Test
    void plainFullScan_emptyFile_returnsNoRows() throws Exception {
        Path file = tempDir.resolve("empty.fastq");
        Files.writeString(file, "", StandardCharsets.UTF_8);

        assertEquals(0,
                readAll(new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf())).size());
    }

    @Test
    void plainFullScan_seekToStart_readsFromBeginning() throws Exception {
        // startByte > 0 but endByte = MAX_VALUE → seeks then uses FastqReader (no plainSplit)
        Path file = tempDir.resolve("seekfull.fastq");
        String rec1 = "@r1\nACGT\n+\nIIII\n"; // 16 bytes
        String rec2 = "@r2\nCCCC\n+\nJJJJ\n";
        Files.writeString(file, rec1 + rec2, StandardCharsets.UTF_8);

        // Starting exactly at the '@' of rec2 → reads only rec2
        FastqInputPartition p = new FastqInputPartition(
                file.toString(), (long) rec1.length(), Long.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r2", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Gzip full scan
    // -------------------------------------------------------------------------

    @Test
    void gzip_fullScan_readsAllRecords() throws Exception {
        Path file = tempDir.resolve("test.fastq.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(file))) {
            os.write(("@r1 d\nACGT\n+\nIIII\n" +
                      "@r2\nCCCC\n+\nJJJJ\n").getBytes(StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(2, rows.size());
        assertEquals("r1",   rows.get(0).getUTF8String(0).toString());
        assertEquals("d",    rows.get(0).getUTF8String(3).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("r2",   rows.get(1).getUTF8String(0).toString());
        assertTrue(rows.get(1).isNullAt(3));
    }

    @Test
    void gzip_fqGzExtension_recognized() throws Exception {
        Path file = tempDir.resolve("test.fq.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(file))) {
            os.write("@r1\nAAAA\n+\nIIII\n".getBytes(StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));

        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Plain split mode (endByte != MAX_VALUE)
    // -------------------------------------------------------------------------

    @Test
    void plainSplit_firstPartition_readsOwnRecord() throws Exception {
        String rec1 = "@r1\nACGT\n+\nIIII\n"; // 16 bytes
        String rec2 = "@r2\nCCCC\n+\nJJJJ\n";
        Path file = tempDir.resolve("split1.fastq");
        Files.writeString(file, rec1 + rec2, StandardCharsets.UTF_8);

        // Partition covers exactly rec1
        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) rec1.length(), conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("IIII", rows.get(0).getUTF8String(2).toString());
    }

    @Test
    void plainSplit_zeroLengthRange_returnsEmpty() throws Exception {
        Path file = tempDir.resolve("zero.fastq");
        Files.writeString(file, "@r1\nACGT\n+\nIIII\n", StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(file.toString(), 0L, 0L, conf());
        assertEquals(0, readAll(p).size());
    }

    @Test
    void plainSplit_startMidFile_advancesToNextRecord() throws Exception {
        // rec1 = 16 bytes; start at byte 8 (mid-line) → advanceToRecordBoundary skips to rec2
        String rec1 = "@r1\nACGT\n+\nIIII\n"; // 16 bytes
        String rec2 = "@r2\nCCCC\n+\nJJJJ\n"; // 16 bytes
        Path file = tempDir.resolve("mid.fastq");
        Files.writeString(file, rec1 + rec2, StandardCharsets.UTF_8);

        long startByte = rec1.length() / 2; // 8 bytes — mid-way through rec1
        long endByte   = (long) (rec1.length() + rec2.length());
        FastqInputPartition p = new FastqInputPartition(
                file.toString(), startByte, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r2", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void plainSplit_multiRecord_allRead() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            sb.append(String.format("@r%d\nACGT\n+\nIIII\n", i));
        }
        String content = sb.toString();
        Path file = tempDir.resolve("multi.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());

        assertEquals(5, readAll(p).size());
    }

    @Test
    void plainSplit_crlfLineEndings_handledGracefully() throws Exception {
        // CRLF (\r\n) endings — \r should be stripped
        Path file = tempDir.resolve("crlf.fastq");
        Files.write(file, "@r1\r\nACGT\r\n+\r\nIIII\r\n".getBytes(StandardCharsets.UTF_8));

        long len = Files.size(file);
        FastqInputPartition p = new FastqInputPartition(file.toString(), 0L, len, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r1",   rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("IIII", rows.get(0).getUTF8String(2).toString());
    }

    // -------------------------------------------------------------------------
    // close() before next()
    // -------------------------------------------------------------------------

    @Test
    void close_beforeNext_doesNotThrow() throws Exception {
        Path file = tempDir.resolve("close.fastq");
        Files.writeString(file, "@r1\nACGT\n+\nIIII\n", StandardCharsets.UTF_8);

        FastqPartitionReader reader = new FastqPartitionReader(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));
        assertDoesNotThrow(reader::close);
    }

    @Test
    void close_afterFullIteration_doesNotThrow() throws Exception {
        Path file = tempDir.resolve("close2.fastq");
        Files.writeString(file, "@r1\nACGT\n+\nIIII\n", StandardCharsets.UTF_8);

        FastqPartitionReader reader = new FastqPartitionReader(
                new FastqInputPartition(file.toString(), 0L, Long.MAX_VALUE, conf()));
        while (reader.next()) { /* drain */ }
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Plain split — blank lines before '@' (exercises while-loop in nextPlainSplit)
    // -------------------------------------------------------------------------

    @Test
    void plainSplit_blankLinesBeforeAt_headerSkipped() throws Exception {
        // File starts with blank lines before the first '@' record.
        // nextPlainSplit() must loop (line 340) until it finds a non-blank '@' header.
        String content = "\n\n@r1\nACGT\n+\nIIII\n";
        Path file = tempDir.resolve("blanks_before.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
    }

    @Test
    void plainSplit_blankLinesBetweenRecords_allRead() throws Exception {
        // Non-standard blank lines between records; both records must be returned.
        String content = "@r1\nACGT\n+\nIIII\n\n\n@r2\nCCCC\n+\nJJJJ\n";
        Path file = tempDir.resolve("blanks_between.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(2, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("r2", rows.get(1).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Plain split — EOF hit while skipping blank/non-'@' lines (line ~293)
    // -------------------------------------------------------------------------

    @Test
    void plainSplit_onlyBlankLines_returnsEmpty() throws Exception {
        // The entire partition contains only blank lines; no '@' header is ever found.
        // nextPlainSplit() loops, then hits null (EOF) inside the while loop → line 293.
        String content = "\n\n\n";
        Path file = tempDir.resolve("all_blanks.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // Plain split — advanceToRecordBoundary reaches EOF without finding '@' (line ~436)
    // -------------------------------------------------------------------------

    @Test
    void plainSplit_startMidFile_noRecordFollows_returnsEmpty() throws Exception {
        // startByte is mid-way through the only record; advanceToRecordBoundary scans to EOF
        // without finding another '@' — exercises the natural-fallthrough path (line 436).
        // nextPlainSplit() then immediately gets null from readLineRaw() and returns false.
        String rec = "@r1\nACGT\n+\nIIII\n"; // 16 bytes
        Path file = tempDir.resolve("mid_eof.fastq");
        Files.writeString(file, rec, StandardCharsets.UTF_8);

        long startByte = 5L; // mid-way through the only record, no '@' follows
        long endByte   = (long) rec.length();
        FastqInputPartition p = new FastqInputPartition(
                file.toString(), startByte, endByte, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // BGZF split mode (isBgzf=true) — uses real BGZF fixture from test resources
    // -------------------------------------------------------------------------

    /** Returns the absolute path to a BGZF-compressed FASTQ fixture. */
    private static java.net.URL bgzfResourceUrl(String name) {
        java.net.URL url = FastqPartitionReaderTest.class.getClassLoader().getResource(name);
        assertNotNull(url, name + " not found in test resources");
        return url;
    }

    private static String bgzfPath() throws Exception {
        return java.nio.file.Paths.get(bgzfResourceUrl(
                "TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz").toURI()).toAbsolutePath().toString();
    }

    @Test
    void bgzf_fullScan_readsAllRecords() throws Exception {
        // isBgzf=true, startByte=0, endByte=MAX_VALUE → reads every record in file.
        // Fixture has exactly 25 000 records (100 000 lines / 4 lines per record).
        String path = bgzfPath();
        FastqInputPartition p = new FastqInputPartition(
                path, 0L, Long.MAX_VALUE, conf(), null, true);

        int count = readAll(p).size();
        assertEquals(25000, count);
    }

    @Test
    void bgzf_twoPartitions_sumEqualsFullScan() throws Exception {
        // Split the file at the first non-zero BGZF block boundary (20135 bytes).
        // Two non-overlapping partitions must together return exactly the same record
        // count as a single full-scan partition (25 000 records).
        String path = bgzfPath();
        long splitByte = 20135L; // first block boundary from the fixture

        FastqInputPartition p1 = new FastqInputPartition(
                path, 0L, splitByte, conf(), null, true);
        FastqInputPartition p2 = new FastqInputPartition(
                path, splitByte, Long.MAX_VALUE, conf(), null, true);

        int count1 = readAll(p1).size();
        int count2 = readAll(p2).size();

        assertTrue(count1 > 0, "first partition should read some records");
        assertTrue(count2 > 0, "second partition should read some records");
        assertEquals(25000, count1 + count2,
                "two BGZF partitions together must cover all records");
    }

    @Test
    void bgzf_partitionStartsAtBlockBoundary_readsCorrectly() throws Exception {
        // A partition whose startByte is exactly at a BGZF block boundary (no scanning needed)
        // should still read records starting from that block.
        String path = bgzfPath();
        long block2Start = 20135L; // offset of second block

        FastqInputPartition p = new FastqInputPartition(
                path, block2Start, Long.MAX_VALUE, conf(), null, true);

        int count = readAll(p).size();
        assertTrue(count > 0, "partition starting at block boundary should read records");
        // Records in second partition plus first block's records must total 30798
        // (verified implicitly by bgzf_twoPartitions_sumEqualsFullScan; here just sanity-check)
        assertTrue(count < 30798, "partition starting at block 2 should not read all records");
    }

    @Test
    void bgzf_partitionPastLastBlock_returnsEmpty() throws Exception {
        // startByte past the EOF block (1957169) — no BGZF magic found in that range,
        // so the partition is immediately exhausted.
        String path = bgzfPath();
        long eofBlockOffset = 1957169L; // last (EOF) block offset in fixture
        long startByte = eofBlockOffset + 1; // one byte past the EOF block

        FastqInputPartition p = new FastqInputPartition(
                path, startByte, Long.MAX_VALUE, conf(), null, true);

        assertEquals(0, readAll(p).size(),
                "partition starting past the EOF block should return no records");
    }

    @Test
    void bgzf_firstRecord_fieldsCorrect() throws Exception {
        // Read only the first BGZF partition and verify the first record's fields
        // are parsed correctly by get().
        String path = bgzfPath();
        FastqInputPartition p = new FastqInputPartition(
                path, 0L, 20135L, conf(), null, true);

        List<InternalRow> rows = readAll(p);
        assertFalse(rows.isEmpty(), "first BGZF partition must contain at least one record");
        InternalRow r = rows.get(0);
        // readName column (0) must be non-empty
        assertFalse(r.getUTF8String(0).toString().isEmpty(), "readName must not be empty");
        // sequence column (1) must be non-empty
        assertFalse(r.getUTF8String(1).toString().isEmpty(), "sequence must not be empty");
        // baseQualities column (2) must be non-empty
        assertFalse(r.getUTF8String(2).toString().isEmpty(), "baseQualities must not be empty");
    }

    @Test
    void bgzf_blockFoundButPastEndByte_returnsEmpty() throws Exception {
        // startByte=1 causes findNextBgzfBlockStart to scan from byte 1 and find the
        // second BGZF block (at ~20135).  endByte=2 is smaller than that block offset,
        // so the `blockStart >= partition.getEndByte()` guard triggers and the partition
        // is immediately marked exhausted — exercising that branch of openBgzfSplit().
        String path = bgzfPath();
        long startByte = 1L;
        long endByte   = 2L; // block at ~20135 >= 2 → exhausted

        FastqInputPartition p = new FastqInputPartition(
                path, startByte, endByte, conf(), null, true);

        assertEquals(0, readAll(p).size(),
                "partition where found BGZF block >= endByte should return no records");
    }

    @Test
    void bgzf_readNumber_propagated() throws Exception {
        // readNumber from the partition should appear in every row (column 4)
        String path = bgzfPath();
        FastqInputPartition p = new FastqInputPartition(
                path, 0L, 20135L, conf(), 1, true);

        List<InternalRow> rows = readAll(p);
        assertFalse(rows.isEmpty());
        for (InternalRow r : rows) {
            assertEquals(1, r.getInt(4), "readNumber should be 1 for every record");
        }
    }

    // -------------------------------------------------------------------------
    // BGZF mode — blank lines (exercises nextBgzf while-loop branches)
    // -------------------------------------------------------------------------

    @Test
    void bgzf_blankLinesBetweenRecords_tolerated() throws Exception {
        // Non-standard blank lines before and between records.
        // nextBgzf() loops (line.isEmpty() → true) until it finds a '@' header,
        // exercising the while-loop body for both the leading and inter-record blanks.
        Path file = tempDir.resolve("blanks_bgzf.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            out.write("\n@r1\nACGT\n+\nIIII\n\n\n@r2\nCCCC\n+\nJJJJ\n"
                    .getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(2, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("r2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void bgzf_trailingBlankLines_whileLoopExhausted() throws Exception {
        // One valid record followed by blank lines then EOF.
        // nextBgzf() reads the blank lines in the while loop until readLineBcis()
        // returns null (EOF), exercising the "header == null" return inside the loop.
        Path file = tempDir.resolve("trailing_blanks_bgzf.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            out.write("@r1\nACGT\n+\nIIII\n\n\n\n"
                    .getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
    }

    @Test
    void bgzf_eofAfterHeader_noSequence_terminatesGracefully() throws Exception {
        // BGZF file truncated after the '@header' line — sequence is null.
        // nextBgzf() reads the header line, then readLineBcis() returns null
        // for the sequence → exhausted = true, return false (sequence == null branch).
        Path file = tempDir.resolve("truncated_bgzf.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            out.write("@r1\n".getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(0, rows.size(), "truncated record (no sequence) should yield no rows");
    }

    @Test
    void bgzf_crlfLineEndings_handledGracefully() throws Exception {
        // BGZF file with CRLF (\r\n) line endings.
        // readLineBcis() reads each byte: when b=='\r', the condition `b != '\r'`
        // is FALSE so the '\r' is not appended to the StringBuilder — covering that branch.
        Path file = tempDir.resolve("crlf_bgzf.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            out.write("@r1\r\nACGT\r\n+\r\nIIII\r\n".getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(1, rows.size(), "BGZF CRLF file should return one record");
        assertEquals("r1",   rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("IIII", rows.get(0).getUTF8String(2).toString());
    }

    @Test
    void bgzf_truncatedAfterSeparator_qualsNull_returnsEmpty() throws Exception {
        // BGZF file ends after the '+' separator — quals line is absent.
        // nextBgzf() reads header, sequence, separator (consumed via readLineBcis),
        // then readLineBcis() returns null for quals → exhausted = true, return false.
        Path file = tempDir.resolve("truncated_bgzf_quals.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            out.write("@r1\nACGT\n+\n".getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(0, rows.size(), "truncated quals should yield no rows");
    }

    @Test
    void bgzf_noTrailingNewline_lastRecordReturned() throws Exception {
        // BGZF file whose quals line has no terminating '\n'.
        // readLineBcis() hits EOF with data in the StringBuilder → sb.length() > 0
        // branch returns the quals string (not null) → record is yielded.
        Path file = tempDir.resolve("no_newline_bgzf.fastq.gz");
        try (BlockCompressedOutputStream out = new BlockCompressedOutputStream(file.toFile())) {
            // No trailing '\n' after quals
            out.write("@r1\nACGT\n+\nIIII".getBytes(StandardCharsets.UTF_8));
        }

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, Long.MAX_VALUE, conf(), null, true);
        List<InternalRow> rows = readAll(p);

        assertEquals(1, rows.size(), "record without trailing newline should still be returned");
        assertEquals("r1",   rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("IIII", rows.get(0).getUTF8String(2).toString());
    }

    // -------------------------------------------------------------------------
    // Plain split — truncated records and no-trailing-newline
    // -------------------------------------------------------------------------

    @Test
    void plainSplit_emptyFile_headerNull_returnsEmpty() throws Exception {
        // Completely empty file — readLineRaw() immediately returns null
        // (no bytes available) before the while loop, so the initial
        // `if (header == null) { return false; }` branch (line 338) is taken.
        Path file = tempDir.resolve("empty.fastq");
        Files.write(file, new byte[0]); // zero bytes
        long endByte = 64L; // > 0 so boundary check doesn't fire first

        FastqInputPartition p = new FastqInputPartition(file.toString(), 0L, endByte, conf());
        assertEquals(0, readAll(p).size(), "empty file should yield no rows");
    }

    @Test
    void plainSplit_nonAtFirstLine_scansPastToRecord() throws Exception {
        // File starts with a non-blank, non-'@' line (e.g., a quality or sequence line).
        // nextPlainSplit() enters the while loop body with header.isEmpty()=FALSE
        // and header.charAt(0) != '@'=TRUE, covering that branch.
        String content = "garbage_line\n@r1\nACGT\n+\nIIII\n";
        Path file = tempDir.resolve("nonAt_first.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());
        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("r1", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void plainSplit_truncatedAfterHeader_sequenceNull_returnsEmpty() throws Exception {
        // File ends after the '@header' line — sequence is null.
        // nextPlainSplit() reads header, then readLineRaw() returns null → returns false.
        String content = "@r1\n";
        Path file = tempDir.resolve("trunc_header.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());
        assertEquals(0, readAll(p).size(), "truncated header-only should yield no rows");
    }

    @Test
    void plainSplit_truncatedAfterSeparator_qualsNull_returnsEmpty() throws Exception {
        // File ends after the '+' separator — quals line is absent.
        // nextPlainSplit() reads header, sequence, separator, then readLineRaw() returns null
        // for quals → exhausted = true, return false.
        String content = "@r1\nACGT\n+\n";
        Path file = tempDir.resolve("trunc_quals.fastq");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());
        assertEquals(0, readAll(p).size(), "truncated quals should yield no rows");
    }

    @Test
    void plainSplit_noTrailingNewline_lastRecordReturned() throws Exception {
        // File whose quals line has no terminating '\n'.
        // readLineRaw() hits EOF with data in sb → sb.length() > 0 branch returns
        // the quals string (not null) → record is yielded.
        String content = "@r1\nACGT\n+\nIIII"; // no trailing '\n'
        Path file = tempDir.resolve("no_newline.fastq");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        FastqInputPartition p = new FastqInputPartition(
                file.toString(), 0L, (long) content.length(), conf());
        List<InternalRow> rows = readAll(p);

        assertEquals(1, rows.size(), "record without trailing newline should still be returned");
        assertEquals("r1",   rows.get(0).getUTF8String(0).toString());
        assertEquals("ACGT", rows.get(0).getUTF8String(1).toString());
        assertEquals("IIII", rows.get(0).getUTF8String(2).toString());
    }
}
