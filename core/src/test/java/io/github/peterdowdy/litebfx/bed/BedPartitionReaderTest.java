package io.github.peterdowdy.litebfx.bed;

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
 * Unit tests for {@link BedPartitionReader}.
 *
 * <p>Tests drive the reader directly (no SparkSession) using in-memory temp files.
 * Coverage targets: plain-text split mode, field parsing helpers, header/comment
 * skipping, and RGB normalisation.
 */
class BedPartitionReaderTest {

    @TempDir
    Path tempDir;

    static Configuration conf() {
        return new Configuration();
    }

    /** Read all rows from a partition into a list. */
    static List<InternalRow> readAll(BedInputPartition p) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        try (BedPartitionReader r = new BedPartitionReader(p)) {
            while (r.next()) {
                rows.add(r.get());
            }
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // Plain-text full scan
    // -------------------------------------------------------------------------

    @Test
    void plainText_fullScan_readsAllRecords() throws Exception {
        Path bed = tempDir.resolve("two.bed");
        Files.writeString(bed, "chr1\t0\t100\nchr2\t200\t300\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void plainText_skipsHeaderAndBlankLines() throws Exception {
        Path bed = tempDir.resolve("header.bed");
        Files.writeString(bed,
                "track name=test\n"
                + "browser position chr1:1-100\n"
                + "#comment line\n"
                + "\n"
                + "chr1\t0\t100\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void plainText_skipsMalformedLinesWithFewerThanThreeColumns() throws Exception {
        Path bed = tempDir.resolve("short.bed");
        Files.writeString(bed,
                "chr1\t0\n"          // only 2 columns — skipped
                + "chr2\t100\t200\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Split mode (startByte / endByte) — exercises readLineFromStream
    // -------------------------------------------------------------------------

    @Test
    void splitMode_startOnLineBoundary_skipsFirstPartition() throws Exception {
        Path bed = tempDir.resolve("split1.bed");
        // line1 = "chr1\t0\t100\n" (11 bytes), line2 = "chr2\t200\t300\n" (13 bytes)
        String line1 = "chr1\t0\t100\n";
        String line2 = "chr2\t200\t300\n";
        Files.writeString(bed, line1 + line2 + "chr3\t400\t500\n", StandardCharsets.UTF_8);

        long startByte = line1.length();         // byte 11, boundary after line1
        long endByte   = startByte + line2.length(); // byte 24, end after line2

        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                startByte, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
        assertEquals(200L, rows.get(0).getLong(1));
    }

    @Test
    void splitMode_startMidLine_discardsPartialLine() throws Exception {
        Path bed = tempDir.resolve("split2.bed");
        String line1 = "chr1\t0\t100\n"; // 11 bytes; byte 4 = '\t' (mid-line)
        String line2 = "chr2\t200\t300\n";
        Files.writeString(bed, line1 + line2, StandardCharsets.UTF_8);

        // startByte = 5 points mid-way through line1; reader must discard remainder
        long startByte = 5L;
        long endByte   = (long) (line1.length() + line2.length());

        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                startByte, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void splitMode_noEndByte_readsToEof() throws Exception {
        Path bed = tempDir.resolve("split3.bed");
        String line1 = "chr1\t0\t100\n"; // 11 bytes
        String line2 = "chr2\t200\t300\n";
        Files.writeString(bed, line1 + line2, StandardCharsets.UTF_8);

        // Start of line2, no endByte limit → reads through EOF via readLineFromStream
        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                (long) line1.length(), Long.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void splitMode_zeroLengthRange_returnsEmpty() throws Exception {
        // endByte = 0 → bedEndByte limit triggers before the first read
        Path bed = tempDir.resolve("split4.bed");
        Files.writeString(bed, "chr1\t0\t100\n", StandardCharsets.UTF_8);

        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                0L, 0L, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // Field parsing — exercised via get()
    // -------------------------------------------------------------------------

    @Test
    void get_badScore_parsesToNull() throws Exception {
        // Score column contains "NaN" — not a valid integer → parseIntOrNull returns null
        Path bed = tempDir.resolve("badscore.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\tNaN\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(4), "score should be null for non-numeric input");
    }

    @Test
    void get_badThickStart_parsesToNull() throws Exception {
        // thickStart column contains "NA" — not a valid long → parseLongOrNull returns null
        Path bed = tempDir.resolve("badthick.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\t+\tNA\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(6), "thickStart should be null for non-numeric input");
    }

    @Test
    void get_itemRgb_packedInteger_normalizedToRgbString() throws Exception {
        // 0xFF0000 = 16711680 (pure red) → parseItemRgb packs it back to "255,0,0"
        Path bed = tempDir.resolve("rgbpacked.bed");
        Files.writeString(bed,
                "chr1\t0\t100\t.\t0\t.\t0\t100\t16711680\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("255,0,0", rows.get(0).getUTF8String(8).toString());
    }

    @Test
    void get_itemRgb_commaForm_passedThrough() throws Exception {
        // "R,G,B" form — passed through unchanged
        Path bed = tempDir.resolve("rgbcomma.bed");
        Files.writeString(bed,
                "chr1\t0\t100\t.\t0\t.\t0\t100\t128,64,32\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("128,64,32", rows.get(0).getUTF8String(8).toString());
    }

    @Test
    void get_scoreDecimal_isNull() throws Exception {
        // A decimal score like "818.0" is not an integer -> null (samtools never truncates
        // BED numeric columns; it does not interpret them at all).
        Path bed = tempDir.resolve("decscore.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t818.0\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(4), "decimal score should be null (no truncation)");
    }

    @Test
    void get_fullBed12Record_allFieldsParsed() throws Exception {
        Path bed = tempDir.resolve("bed12.bed");
        // A full BED12 record with all columns present
        Files.writeString(bed,
                "chr1\t0\t1000\tfeature\t600\t+\t200\t800\t0,128,255\t3\t100,200,300,\t0,300,700,\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertEquals("chr1",    r.getUTF8String(0).toString());
        assertEquals(0L,        r.getLong(1));
        assertEquals(1000L,     r.getLong(2));
        assertEquals("feature", r.getUTF8String(3).toString());
        assertEquals(600,       r.getInt(4));
        assertEquals("+",       r.getUTF8String(5).toString());
        assertEquals(200L,      r.getLong(6));
        assertEquals(800L,      r.getLong(7));
        assertEquals("0,128,255", r.getUTF8String(8).toString()); // comma form
        assertEquals(3,         r.getInt(9));
        assertEquals("100,200,300", r.getUTF8String(10).toString()); // trailing comma stripped
        assertEquals("0,300,700",   r.getUTF8String(11).toString());
    }

    // -------------------------------------------------------------------------
    // Field parsing — chromStart == chromEnd (zero-length region, valid in BED)
    // -------------------------------------------------------------------------

    @Test
    void get_chromStartEqualsChromEnd_zeroLengthRegionParsed() throws Exception {
        // BED allows chromStart == chromEnd to represent an insertion point
        Path bed = tempDir.resolve("zero_len.bed");
        Files.writeString(bed, "chr1\t500\t500\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertEquals(500L, r.getLong(1));
        assertEquals(500L, r.getLong(2));
    }

    // -------------------------------------------------------------------------
    // Field parsing — 3-column minimal BED (no optional fields)
    // -------------------------------------------------------------------------

    @Test
    void get_minimalBed3_optionalColumnsAreNull() throws Exception {
        Path bed = tempDir.resolve("bed3.bed");
        Files.writeString(bed, "chrM\t0\t16569\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertEquals("chrM", r.getUTF8String(0).toString());
        assertEquals(0L,     r.getLong(1));
        assertEquals(16569L, r.getLong(2));
        // Optional columns (3..11) must all be null
        for (int col = 3; col <= 11; col++) {
            assertTrue(r.isNullAt(col), "column " + col + " should be null in 3-col BED");
        }
    }

    // -------------------------------------------------------------------------
    // Split mode — CRLF line endings stripped correctly
    // -------------------------------------------------------------------------

    @Test
    void splitMode_crlfLineEndings_strippedCorrectly() throws Exception {
        Path bed = tempDir.resolve("crlf.bed");
        // Write raw bytes with CRLF endings
        byte[] content = "chr1\t0\t100\r\nchr2\t200\t300\r\n".getBytes(StandardCharsets.UTF_8);
        Files.write(bed, content);

        long len = content.length;
        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                0L, len + 1, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals(100L,   rows.get(0).getLong(2));
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
        assertEquals(300L,   rows.get(1).getLong(2));
    }

    // -------------------------------------------------------------------------
    // close() after full iteration
    // -------------------------------------------------------------------------

    @Test
    void close_afterFullIteration_doesNotThrow() throws Exception {
        Path bed = tempDir.resolve("close.bed");
        Files.writeString(bed, "chr1\t0\t100\n", StandardCharsets.UTF_8);

        BedPartitionReader reader = new BedPartitionReader(
                new BedInputPartition(bed.toString(), null, conf()));
        while (reader.next()) { /* drain */ }
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // next() called multiple times after exhaustion returns false
    // -------------------------------------------------------------------------

    @Test
    void next_calledMultipleTimesAfterExhaustion_returnsFalse() throws Exception {
        Path bed = tempDir.resolve("exhaust.bed");
        Files.writeString(bed, "chr1\t0\t100\n", StandardCharsets.UTF_8);

        BedPartitionReader reader = new BedPartitionReader(
                new BedInputPartition(bed.toString(), null, conf()));
        try {
            assertTrue(reader.next());
            assertFalse(reader.next()); // first exhaustion
            assertFalse(reader.next()); // must not throw
        } finally {
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // Field parsing — score = 0 is valid (not treated as null)
    // -------------------------------------------------------------------------

    @Test
    void get_scoreZero_isNotNull() throws Exception {
        Path bed = tempDir.resolve("score0.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(4), "score=0 should not be null");
        assertEquals(0, rows.get(0).getInt(4));
    }

    // -------------------------------------------------------------------------
    // BGZF full-scan (no tabix index, no region filter)
    // -------------------------------------------------------------------------

    @Test
    void bgzf_fullScan_readsAllRecords() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path = fx.bed6Bgzf().toString();

        // indexPath=null, queryChrom=null → full BGZF scan
        List<InternalRow> rows = readAll(new BedInputPartition(path, null, conf()));

        assertEquals(BedTestGenerator.BED6_TOTAL, rows.size());
        // First record should be chr1 peak1
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals(100L,   rows.get(0).getLong(1));
        assertEquals(200L,   rows.get(0).getLong(2));
    }

    @Test
    void bgzf_fullScan_allFieldsParsed() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path = fx.bed6Bgzf().toString();

        List<InternalRow> rows = readAll(new BedInputPartition(path, null, conf()));

        // BED6: chrom, chromStart, chromEnd, name, score, strand
        InternalRow r = rows.get(0); // chr1 100 200 peak1 500 +
        assertEquals("chr1",  r.getUTF8String(0).toString());
        assertEquals(100L,    r.getLong(1));
        assertEquals(200L,    r.getLong(2));
        assertEquals("peak1", r.getUTF8String(3).toString());
        assertEquals(500,     r.getInt(4));
        assertEquals("+",     r.getUTF8String(5).toString());
        // Columns 6–11 (BED6 has no thickStart etc.) must be null
        for (int col = 6; col <= 11; col++) {
            assertTrue(r.isNullAt(col), "column " + col + " should be null in BED6");
        }
    }

    // -------------------------------------------------------------------------
    // Tabix region query (BGZF file + .tbi index + queryChrom)
    // -------------------------------------------------------------------------

    @Test
    void tabix_regionQuery_chr1_returnsOnlyChr1Records() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path  = fx.bed6Bgzf().toString();
        String index = fx.bed6Tbi().toString();

        // Query chr1, full range
        BedInputPartition p = new BedInputPartition(
                path, index, "chr1", 0L, Long.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(BedTestGenerator.BED6_CHR1_COUNT, rows.size());
        for (InternalRow r : rows) {
            assertEquals("chr1", r.getUTF8String(0).toString());
        }
    }

    @Test
    void tabix_regionQuery_chr2_returnsOnlyChr2Records() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path  = fx.bed6Bgzf().toString();
        String index = fx.bed6Tbi().toString();

        BedInputPartition p = new BedInputPartition(
                path, index, "chr2", 0L, Long.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(BedTestGenerator.BED6_CHR2_COUNT, rows.size());
        for (InternalRow r : rows) {
            assertEquals("chr2", r.getUTF8String(0).toString());
        }
    }

    @Test
    void tabix_regionQuery_posRange_returnsOnlyChr1() throws Exception {
        // chr1 records: [100,200), [500,700), [1000,1200)
        // Tabix block-level filtering returns the blocks that *overlap* [500, MAX),
        // which may include earlier records sharing the same BGZF block.
        // The partition reader returns all records in those blocks; record-level
        // position filtering is applied by Spark above this layer.
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path  = fx.bed6Bgzf().toString();
        String index = fx.bed6Tbi().toString();

        BedInputPartition p = new BedInputPartition(
                path, index, "chr1", 500L, Long.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);

        // At minimum the records at/after pos 500 must be present; there may be more
        // from the same BGZF block.
        assertTrue(rows.size() >= BedTestGenerator.BED6_CHR1_FROM_500,
                "should return at least " + BedTestGenerator.BED6_CHR1_FROM_500 + " chr1 records");
        // All returned records must be on chr1 (chrom filter is precise).
        for (InternalRow r : rows) {
            assertEquals("chr1", r.getUTF8String(0).toString());
        }
    }

    // -------------------------------------------------------------------------
    // Tabix region query — chromosome not in file (empty block list)
    // -------------------------------------------------------------------------

    @Test
    void tabix_regionQuery_unknownChrom_returnsEmpty() throws Exception {
        // Querying a contig that has no tabix blocks → tabixBlocks.isEmpty() → returns nothing
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path  = fx.bed6Bgzf().toString();
        String index = fx.bed6Tbi().toString();

        BedInputPartition p = new BedInputPartition(
                path, index, "chrX", 0L, Long.MAX_VALUE, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // Tabix close() before any next()
    // -------------------------------------------------------------------------

    @Test
    void tabix_close_beforeNext_doesNotThrow() throws Exception {
        BedTestGenerator.Fixtures fx = BedTestGenerator.generate(tempDir);
        String path  = fx.bed6Bgzf().toString();
        String index = fx.bed6Tbi().toString();

        BedPartitionReader reader = new BedPartitionReader(
                new BedInputPartition(path, index, "chr1", 0L, Long.MAX_VALUE, conf()));
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Regular gzip (non-BGZF) .gz file — exercises the GZIPInputStream branch
    // -------------------------------------------------------------------------

    @Test
    void regularGzip_fullScan_readsAllRecords() throws Exception {
        // Write a plain GZIP file (not BGZF): isBgzfStream() returns false,
        // so open() takes the `else if (looksCompressed(path))` branch.
        Path bed = tempDir.resolve("regular.bed.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(bed))) {
            os.write("chr1\t0\t100\nchr2\t200\t300\n".getBytes(StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Field parsing — name = "." maps to null
    // -------------------------------------------------------------------------

    @Test
    void get_nameDot_isNull() throws Exception {
        Path bed = tempDir.resolve("namedot.bed");
        Files.writeString(bed, "chr1\t0\t100\t.\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(3), "name '.' should be null");
    }

    // -------------------------------------------------------------------------
    // Field parsing — itemRgb = "0" maps to null
    // -------------------------------------------------------------------------

    @Test
    void get_itemRgb_zero_isNull() throws Exception {
        // The "0" sentinel value means "no colour" — must be stored as null.
        Path bed = tempDir.resolve("rgbzero.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\t.\t0\t100\t0\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(8), "itemRgb '0' should be null");
    }

    // -------------------------------------------------------------------------
    // Field parsing — itemRgb = "" (empty string) maps to null
    // -------------------------------------------------------------------------

    @Test
    void get_itemRgb_empty_isNull() throws Exception {
        // An empty col 8 (tab followed immediately by another tab) must map to null.
        Path bed = tempDir.resolve("rgbempty.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\t.\t0\t100\t\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(8), "empty itemRgb should be null");
    }

    // -------------------------------------------------------------------------
    // Field parsing — parseItemRgb with non-numeric, non-comma string
    //                 exercises the NumberFormatException catch branch
    // -------------------------------------------------------------------------

    @Test
    void get_itemRgb_invalidString_returnedAsIs() throws Exception {
        // "blue" is not a packed integer and has no comma, so parseInt throws →
        // the catch block returns the original string unchanged.
        Path bed = tempDir.resolve("rgbinvalid.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\t.\t0\t100\tblue\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("blue", rows.get(0).getUTF8String(8).toString());
    }

    // -------------------------------------------------------------------------
    // Field parsing — parseLongOrNull rejects decimals (e.g. "200.5" → null)
    // -------------------------------------------------------------------------

    @Test
    void get_thickStartDecimal_isNull() throws Exception {
        // thickStart = "200.5" is not a long -> null (no truncation; matches samtools).
        Path bed = tempDir.resolve("thickdec.bed");
        Files.writeString(bed, "chr1\t0\t1000\tname\t0\t+\t200.5\t800\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(6), "decimal thickStart should be null (no truncation)");
    }

    // -------------------------------------------------------------------------
    // looksCompressed — .bgz and .bgzf extensions
    // -------------------------------------------------------------------------

    @Test
    void bgzExtension_bgzfContent_readsAllRecords() throws Exception {
        // A BGZF file named ".bgz" — looksCompressed() returns true for ".bgz",
        // isBgzfStream() returns true → takes the BGZF branch in open().
        Path bed = tempDir.resolve("test.bed.bgz");
        try (htsjdk.samtools.util.BlockCompressedOutputStream out =
                 new htsjdk.samtools.util.BlockCompressedOutputStream(bed.toFile())) {
            out.write("chr1\t0\t100\nchr2\t200\t300\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void bgzfExtension_bgzfContent_readsAllRecords() throws Exception {
        // A BGZF file named ".bgzf" — looksCompressed() returns true for ".bgzf",
        // isBgzfStream() returns true → takes the BGZF branch in open().
        Path bed = tempDir.resolve("test.bed.bgzf");
        try (htsjdk.samtools.util.BlockCompressedOutputStream out =
                 new htsjdk.samtools.util.BlockCompressedOutputStream(bed.toFile())) {
            out.write("chr1\t0\t100\nchr2\t200\t300\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void isBgzfStream_shortFile_returnsNonBgzf_readsViaGzip() throws Exception {
        // A gzip file that is so small isBgzfStream reads fewer than 18 bytes,
        // returning false → open() falls back to GZIPInputStream.
        // We write a very small gzip that is not BGZF but decompresses correctly.
        Path bed = tempDir.resolve("tiny.bed.gz");
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(bed))) {
            os.write("chr1\t0\t5\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // isBgzfStream() — raw-byte tests for the missed branches in the compound
    // BGZF header check. Each file is named .bed.gz (looksCompressed=true) so
    // isBgzfStream is called, but the content is crafted to trigger a specific
    // false-branch. All non-valid-gzip content causes GZIPInputStream to throw.
    // -------------------------------------------------------------------------

    @Test
    void isBgzfStream_rawFileLessThan18Bytes_returnsFalse() throws Exception {
        // File has fewer than 18 bytes → `in.read(header, 0, 18)` returns < 18
        // → `if (read < 18) return false` (the TRUE branch of that check, missed so far).
        Path bed = tempDir.resolve("raw_short.bed.gz");
        Files.write(bed, new byte[10]); // 10 zero bytes, not valid GZIP

        assertThrows(Exception.class, () ->
                readAll(new BedInputPartition(bed.toString(), null, conf())),
                "corrupt tiny .gz file should throw while opening");
    }

    @Test
    void isBgzfStream_firstByteNotGzipMagic_returnsFalse() throws Exception {
        // header[0] = 0x00 ≠ 0x1F → short-circuit, return false
        // (covers the `header[0] == 0x1F` → FALSE branch of the compound condition).
        Path bed = tempDir.resolve("raw_nongzip1.bed.gz");
        Files.write(bed, new byte[18]); // all zeros, header[0] = 0x00

        assertThrows(Exception.class, () ->
                readAll(new BedInputPartition(bed.toString(), null, conf())),
                "file not starting with 0x1F should throw as non-GZIP");
    }

    @Test
    void isBgzfStream_firstByteGzipMagicSecondNotGzip_returnsFalse() throws Exception {
        // header[0] = 0x1F (first GZIP magic byte) but header[1] = 0x00 ≠ 0x8B
        // → second condition FALSE, return false (covers `header[1] == 0x8B` → FALSE).
        Path bed = tempDir.resolve("raw_nongzip2.bed.gz");
        byte[] content = new byte[18];
        content[0] = (byte) 0x1F;
        // content[1] = 0x00 (not 0x8B) → B=FALSE
        Files.write(bed, content);

        assertThrows(Exception.class, () ->
                readAll(new BedInputPartition(bed.toString(), null, conf())));
    }

    @Test
    void isBgzfStream_fextraSetButWrongSI1_returnsFalse() throws Exception {
        // GZIP magic + FEXTRA flag set (header[3] bit 2 = 1) but header[12] ≠ 'B'
        // → `header[12] == 0x42` FALSE → return false (covers D=FALSE branch).
        // Bytes: 1F 8B 08 04 00 00 00 00 00 FF 06 00 00 00 ...
        // (header[3]=0x04=FEXTRA, header[12]=0x00≠0x42)
        Path bed = tempDir.resolve("raw_fextra_wrongsi1.bed.gz");
        byte[] content = new byte[18];
        content[0] = (byte) 0x1F;
        content[1] = (byte) 0x8B;
        content[3] = (byte) 0x04; // FEXTRA flag set
        // header[12] stays 0x00 ≠ 0x42
        Files.write(bed, content);

        assertThrows(Exception.class, () ->
                readAll(new BedInputPartition(bed.toString(), null, conf())));
    }

    @Test
    void isBgzfStream_fextraAndSI1CorrectButWrongSI2_returnsFalse() throws Exception {
        // header[12] = 0x42 ('B') but header[13] = 0x00 ≠ 0x43 ('C')
        // → `header[13] == 0x43` FALSE → return false (covers E=FALSE branch).
        Path bed = tempDir.resolve("raw_fextra_wrongsi2.bed.gz");
        byte[] content = new byte[18];
        content[0] = (byte) 0x1F;
        content[1] = (byte) 0x8B;
        content[3] = (byte) 0x04; // FEXTRA flag set
        content[12] = (byte) 0x42; // SI1 = 'B' ✓
        // content[13] = 0x00 ≠ 0x43 → E=FALSE
        Files.write(bed, content);

        assertThrows(Exception.class, () ->
                readAll(new BedInputPartition(bed.toString(), null, conf())));
    }

    // -------------------------------------------------------------------------
    // Split mode — startByte >= fileLength → early return in open()
    // -------------------------------------------------------------------------

    @Test
    void splitMode_startBytePastEof_returnsEmpty() throws Exception {
        // startByte past the end of file triggers the early-return guard in open().
        // endByte = 0 ensures bedEndByte=0 so readLine() returns null immediately
        // even though fsIn is left at position 0 after the early return.
        Path bed = tempDir.resolve("pasteof.bed");
        Files.writeString(bed, "chr1\t0\t100\n", StandardCharsets.UTF_8);
        long fileLen = Files.size(bed);

        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                fileLen + 100, 0L, conf());

        assertEquals(0, readAll(p).size(), "partition past EOF should return no records");
    }

    // -------------------------------------------------------------------------
    // parseIntOrNull — empty-after-trim branch (s.isEmpty() = true → null)
    // -------------------------------------------------------------------------

    @Test
    void get_scoreWhitespace_parsesToNull() throws Exception {
        // score column = "   " (spaces only) → parseIntOrNull: s.trim() = "" →
        // s.isEmpty() = true → returns null.
        Path bed = tempDir.resolve("whitespace_score.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t   \n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(4), "whitespace-only score should be null");
    }

    // -------------------------------------------------------------------------
    // parseLongOrNull — empty-after-trim branch (s.isEmpty() = true → null)
    // -------------------------------------------------------------------------

    @Test
    void get_thickStartWhitespace_parsesToNull() throws Exception {
        // thickStart column = "   " → parseLongOrNull: s.trim() = "" →
        // s.isEmpty() = true → returns null.
        Path bed = tempDir.resolve("whitespace_thick.bed");
        Files.writeString(bed, "chr1\t0\t100\tname\t0\t+\t   \t800\n", StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(6), "whitespace-only thickStart should be null");
    }

    // -------------------------------------------------------------------------
    // get() — n > 10 && c[10].trim().isEmpty() = true → values[10] = null
    // -------------------------------------------------------------------------

    @Test
    void get_emptyBlockSizes_isNull() throws Exception {
        // 11 columns present, column 10 (blockSizes) is an empty string →
        // !c[10].trim().isEmpty() = false → values[10] = null.
        Path bed = tempDir.resolve("empty_blocksizes.bed");
        Files.writeString(bed, "chr1\t0\t1000\tfeat\t0\t+\t0\t1000\t0\t3\t\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(10), "empty blockSizes column should be null");
        assertEquals(3, rows.get(0).getInt(9)); // blockCount is still parsed
    }

    // -------------------------------------------------------------------------
    // get() — n > 11 && c[11].trim().isEmpty() = true → values[11] = null
    // -------------------------------------------------------------------------

    @Test
    void get_emptyBlockStarts_isNull() throws Exception {
        // 12 columns present, column 11 (blockStarts) is an empty string →
        // !c[11].trim().isEmpty() = false → values[11] = null.
        Path bed = tempDir.resolve("empty_blockstarts.bed");
        Files.writeString(bed, "chr1\t0\t1000\tfeat\t0\t+\t0\t1000\t0\t3\t100,200,50\t\n",
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(10), "blockSizes should not be null");
        assertTrue(rows.get(0).isNullAt(11), "empty blockStarts column should be null");
    }

    // -------------------------------------------------------------------------
    // next() — malformed line with fewer than 3 fields
    // -------------------------------------------------------------------------

    @Test
    void next_malformedLine_lessThan3Fields_skipped() throws Exception {
        // A line with only 2 tab-separated fields is malformed (BED3 minimum is 3).
        // next() splits on '\t', gets length < 3 → continues the loop and skips the line.
        // The valid line that follows must still be returned.
        Path bed = tempDir.resolve("malformed.bed");
        Files.writeString(bed,
                "chr1\tonly_two_fields\n" +       // malformed: length < 3 → skipped
                "chr2\t100\t200\n",               // valid BED3
                StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(new BedInputPartition(bed.toString(), null, conf()));
        assertEquals(1, rows.size(), "malformed line should be skipped; valid line returned");
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // readLineFromStream() — EOF without trailing newline (sb.length() > 0 branch)
    // -------------------------------------------------------------------------

    @Test
    void next_noTrailingNewline_lastRecordReturned() throws Exception {
        // Plain BED file whose last line has no terminating '\n'.
        // Must use split mode (startByte>0 or endByte!=MAX_VALUE) so that readLine()
        // delegates to readLineFromStream() instead of BufferedReader.readLine().
        // readLineFromStream() hits EOF with content in the StringBuilder →
        // sb.length() > 0 branch TRUE → returns the content (not null) → record yielded.
        Path bed = tempDir.resolve("no_newline.bed");
        Files.write(bed, "chr1\t0\t100".getBytes(StandardCharsets.UTF_8)); // no trailing '\n'
        long fileLen = Files.size(bed);

        // endByte > fileLen triggers isBedSplitMode; boundary check uses fsIn.getPos() so
        // it only fires at MAX_VALUE or actual end, leaving readLineFromStream to hit EOF.
        List<InternalRow> rows = readAll(new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                0L, fileLen + 100, conf()));
        assertEquals(1, rows.size(), "record without trailing newline should still be returned");
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals(0L, rows.get(0).getLong(1));
        assertEquals(100L, rows.get(0).getLong(2));
    }

    // -------------------------------------------------------------------------
    // open() — startByte mid-line, file ends without newline (b==-1 in skip loop)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_startMidLine_fileEndsNoNewline_emptyPartition() throws Exception {
        // File has startByte mid-way through a line and no '\n' terminator.
        // The partial-line skip while loop (b = fsIn.read()) reads until EOF (b==-1)
        // without ever finding '\n', covering the b==-1 exit branch.
        // After the skip, fsIn is at EOF, so readLineFromStream returns null → 0 records.
        Path bed = tempDir.resolve("midline_eof.bed");
        Files.write(bed, "chr1\t0\t100".getBytes(StandardCharsets.UTF_8)); // 10 bytes, no '\n'

        // startByte=5 → peek at byte 4 ('1' from '100'), not '\n' → enter skip loop
        long startByte = 5L;
        long endByte   = Files.size(bed);
        BedInputPartition p = new BedInputPartition(
                bed.toString(), null, null, 0L, Long.MAX_VALUE,
                startByte, endByte, conf());

        assertEquals(0, readAll(p).size(), "mid-line start with no trailing newline yields no records");
    }
}
