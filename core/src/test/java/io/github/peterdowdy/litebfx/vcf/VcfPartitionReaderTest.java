package io.github.peterdowdy.litebfx.vcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VcfPartitionReader}.
 *
 * <p>Tests drive the reader directly (no SparkSession) using in-memory temp files.
 * Coverage targets:
 * <ul>
 *   <li>Split mode ({@code openSplitMode} / {@code nextSplitMode} / {@code getSplitMode})
 *       including field parsing, {@code parseInfoString}, and {@code parseSampleNames}.</li>
 *   <li>Full-file VCFFileReader path ({@code getFromVariantContext}).</li>
 *   <li>End-byte boundary and mid-file split (partial-line skip).</li>
 *   <li>{@code close()} before {@code next()}.</li>
 * </ul>
 */
class VcfPartitionReaderTest {

    @TempDir
    Path tempDir;

    static Configuration conf() {
        return new Configuration();
    }

    /** Read all rows from a partition into a list. */
    static List<InternalRow> readAll(VcfInputPartition p) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        try (VcfPartitionReader r = new VcfPartitionReader(p)) {
            while (r.next()) {
                rows.add(r.get());
            }
        }
        return rows;
    }

    /**
     * Creates a VcfInputPartition that uses split mode (line-based parsing).
     * Sets endByte just past the file size so all data lines are read.
     */
    static VcfInputPartition splitFull(Path file) throws IOException {
        long len = Files.size(file);
        return new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, len + 1, conf());
    }

    static Path writeVcf(Path dir, String name, String content) throws IOException {
        Path file = dir.resolve(name);
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    /** Sites-only VCF header (no FORMAT/sample columns). */
    static final String HEADER =
            "##fileformat=VCFv4.2\n" +
            "##contig=<ID=chr1,length=1000000>\n" +
            "##contig=<ID=chr2,length=1000000>\n" +
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n";

    /** VCF header with one sample column. */
    static final String HEADER_WITH_SAMPLE =
            "##fileformat=VCFv4.2\n" +
            "##contig=<ID=chr1,length=1000000>\n" +
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample1\n";

    // -------------------------------------------------------------------------
    // Split mode — basic field parsing via getSplitMode()
    // -------------------------------------------------------------------------

    @Test
    void splitMode_basicFields_parsedCorrectly() throws Exception {
        String data = "chr1\t100\trs1\tA\tT\t30.5\tPASS\tDP=50;AF=0.5\n";
        Path file = writeVcf(tempDir, "basic.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertEquals("chr1", r.getUTF8String(0).toString()); // chrom
        assertEquals(100,    r.getInt(1));                   // pos
        assertEquals("rs1",  r.getUTF8String(2).toString()); // id
        assertEquals("A",    r.getUTF8String(3).toString()); // ref
        assertEquals("T",    r.getArray(4).getUTF8String(0).toString()); // alt[0]
        assertEquals(30.5,   r.getDouble(5), 0.001);         // qual
        assertEquals("PASS", r.getUTF8String(6).toString()); // filter
        assertFalse(r.isNullAt(7));                          // info map present
        assertTrue(r.isNullAt(8));                           // format absent (sites-only)
        assertTrue(r.isNullAt(9));                           // genotypes absent
    }

    @Test
    void splitMode_multipleRecords_countCorrect() throws Exception {
        String data =
                "chr1\t100\trs1\tA\tT\t30\tPASS\tDP=10\n" +
                "chr1\t200\t.\tG\tC\t.\t.\t.\n" +
                "chr2\t300\t.\tT\tA\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "multi.vcf", HEADER + data);

        assertEquals(3, readAll(splitFull(file)).size());
    }

    // -------------------------------------------------------------------------
    // Split mode — dot-valued fields map to null
    // -------------------------------------------------------------------------

    @Test
    void splitMode_dotFields_mappedToNull() throws Exception {
        String data = "chr1\t100\t.\tA\t.\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "dots.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertTrue(r.isNullAt(2)); // id = null
        assertTrue(r.isNullAt(4)); // alt = null
        assertTrue(r.isNullAt(5)); // qual = null
        assertTrue(r.isNullAt(6)); // filter = null
    }

    @Test
    void splitMode_qualInvalid_nullInRow() throws Exception {
        // Non-numeric QUAL should silently map to null
        String data = "chr1\t100\t.\tA\tT\tNOTANUM\t.\t.\n";
        Path file = writeVcf(tempDir, "badqual.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(5));
    }

    // -------------------------------------------------------------------------
    // Split mode — parseInfoString
    // -------------------------------------------------------------------------

    @Test
    void splitMode_infoKeyValue_presentInMap() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\tDP=30;AF=0.25\n";
        Path file = writeVcf(tempDir, "info.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(7)); // map is non-null
    }

    @Test
    void splitMode_infoFlagField_mappedToTrue() throws Exception {
        // Flag fields (no '=') should be mapped to "true"
        String data = "chr1\t100\t.\tA\tT\t.\t.\tSOMEFLAG\n";
        Path file = writeVcf(tempDir, "flag.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(7));
    }

    @Test
    void splitMode_infoDot_emptyMap() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "infodot.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        // An empty/dot INFO still yields a non-null (empty) map
        assertFalse(rows.get(0).isNullAt(7));
    }

    // -------------------------------------------------------------------------
    // Split mode — parseSampleNames + genotype columns
    // -------------------------------------------------------------------------

    @Test
    void splitMode_withSample_genotypesPopulated() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\tGT\t0/1\n";
        Path file = writeVcf(tempDir, "samples.vcf", HEADER_WITH_SAMPLE + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertFalse(r.isNullAt(8)); // format = "GT"
        assertFalse(r.isNullAt(9)); // genotypes map (sample1 → "0/1")
        assertEquals("GT", r.getUTF8String(8).toString());
    }

    @Test
    void splitMode_noSamplesNoFormat_genotypesNull() throws Exception {
        // Sites-only VCF: cols 8+ absent → format and genotypes are null
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "sitesonly.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(8)); // format
        assertTrue(rows.get(0).isNullAt(9)); // genotypes
    }

    // -------------------------------------------------------------------------
    // Split mode — line filtering (comments, blank lines, malformed)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_skipsCommentAndBlankLines() throws Exception {
        String content = HEADER +
                "#extra comment line\n" +
                "\n" +
                "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "skip.vcf", content);

        assertEquals(1, readAll(splitFull(file)).size());
    }

    @Test
    void splitMode_skipsMalformedLinesFewerThan8Cols() throws Exception {
        String content = HEADER +
                "chr1\t100\n" +                    // only 2 columns — skipped
                "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "malformed.vcf", content);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Split mode — byte-range boundaries
    // -------------------------------------------------------------------------

    @Test
    void splitMode_endByte_stopsAtBoundary() throws Exception {
        String line1 = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        String line2 = "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "boundary.vcf", HEADER + line1 + line2);

        long headerLen = HEADER.getBytes(StandardCharsets.UTF_8).length;
        // endByte at start of line2 → line1 read, line2 not
        long endByte = headerLen + line1.getBytes(StandardCharsets.UTF_8).length;

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void splitMode_midFileSplit_skipsPartialLine() throws Exception {
        String line1 = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        String line2 = "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "mid.vcf", HEADER + line1 + line2);

        long headerLen = HEADER.getBytes(StandardCharsets.UTF_8).length;
        // Start mid-way through line1 → partial-line skip → reads only line2
        long startByte = headerLen + line1.length() / 2;
        long endByte   = headerLen + line1.length() + line2.length() + 1;

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                startByte, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void splitMode_startByteAtLineBoundary_readsCorrectly() throws Exception {
        String line1 = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        String line2 = "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "lineboundary.vcf", HEADER + line1 + line2);

        long headerLen  = HEADER.getBytes(StandardCharsets.UTF_8).length;
        // startByte points exactly at the newline at end of line1 → prev byte '\n' → no skip
        long startByte  = headerLen + line1.length() - 1; // byte index of '\n'
        long endByte    = headerLen + line1.length() + line2.length() + 1;

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                startByte, endByte, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    @Test
    void splitMode_startBytePastEof_returnsEmpty() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "pasteof.vcf", HEADER + data);

        long len = Files.size(file);
        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                len + 100, len + 200, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // VCFFileReader full-scan path (getFromVariantContext)
    // -------------------------------------------------------------------------

    @Test
    void vcfFileReader_fullScan_readsAllVariants() throws Exception {
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\trs1\tA\tT\t30\tPASS\tDP=50\n" +
                "chr1\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "reader.vcf", vcf);

        // Full-file partition: startByte=0, endByte=MAX_VALUE → VCFFileReader path
        VcfInputPartition p = new VcfInputPartition(file.toString(), null, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(2, rows.size());

        // Row 0: rs1 / PASS / qual 30
        InternalRow r0 = rows.get(0);
        assertEquals("chr1", r0.getUTF8String(0).toString());
        assertEquals(100,    r0.getInt(1));
        assertEquals("rs1",  r0.getUTF8String(2).toString());
        assertEquals("A",    r0.getUTF8String(3).toString());
        assertEquals("T",    r0.getArray(4).getUTF8String(0).toString());
        assertFalse(r0.isNullAt(5));                          // qual present
        assertEquals("PASS", r0.getUTF8String(6).toString()); // PASS filter

        // Row 1: id="."/no qual/no filter → all null
        InternalRow r1 = rows.get(1);
        assertEquals("chr1", r1.getUTF8String(0).toString());
        assertEquals(200,    r1.getInt(1));
        assertTrue(r1.isNullAt(2)); // id null
        assertTrue(r1.isNullAt(5)); // qual null
        assertTrue(r1.isNullAt(6)); // filter null
    }

    @Test
    void vcfFileReader_multiAltAlleles_returnedAsArray() throws Exception {
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\tT,G\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "multialt.vcf", vcf);

        List<InternalRow> rows = readAll(new VcfInputPartition(file.toString(), null, conf()));

        assertEquals(1, rows.size());
        ArrayData alt = rows.get(0).getArray(4);
        assertEquals(2, alt.numElements());
        assertEquals("T", alt.getUTF8String(0).toString());
        assertEquals("G", alt.getUTF8String(1).toString());
    }

    // -------------------------------------------------------------------------
    // close() before next()
    // -------------------------------------------------------------------------

    @Test
    void close_beforeNext_splitMode_doesNotThrow() throws Exception {
        Path file = writeVcf(tempDir, "close1.vcf",
                HEADER + "chr1\t100\t.\tA\tT\t.\t.\t.\n");

        VcfPartitionReader reader = new VcfPartitionReader(splitFull(file));
        assertDoesNotThrow(reader::close);
    }

    @Test
    void close_beforeNext_fullScanMode_doesNotThrow() throws Exception {
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "close2.vcf", vcf);

        VcfPartitionReader reader = new VcfPartitionReader(
                new VcfInputPartition(file.toString(), null, conf()));
        assertDoesNotThrow(reader::close);
    }

    @Test
    void close_afterFullIteration_doesNotThrow() throws Exception {
        Path file = writeVcf(tempDir, "close3.vcf",
                HEADER + "chr1\t100\t.\tA\tT\t.\t.\t.\n");

        VcfPartitionReader reader = new VcfPartitionReader(splitFull(file));
        while (reader.next()) { /* drain */ }
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Multi-chrom mode — queryChroms with no index (full-scan fallback)
    // -------------------------------------------------------------------------

    @Test
    void multiChromMode_noIndex_fullScanFallback_readsAllVariants() throws Exception {
        // queryChroms set, indexPath=null → openMultiChromMode() falls back to full-file scan.
        // Covers openMultiChromMode() fallback path (lines ~217-220).
        String data =
                "chr1\t100\t.\tA\tT\t.\t.\t.\n" +
                "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "multichrom.vcf", HEADER + data);

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, new String[]{"chr1", "chr2"}, conf());

        List<InternalRow> rows = readAll(p);
        // Full-scan fallback reads all records (both chroms)
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
    }

    @Test
    void multiChromMode_noIndex_emptyVcf_returnsEmpty() throws Exception {
        // queryChroms with no data lines → zero rows
        Path file = writeVcf(tempDir, "multichrom_empty.vcf", HEADER);

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, new String[]{"chr1"}, conf());

        assertEquals(0, readAll(p).size());
    }

    @Test
    void multiChromMode_badIndex_fallsBackToFullScan() throws Exception {
        // queryChroms with an indexPath pointing to a non-tabix file →
        // VCFFileReader constructor throws → exception caught → falls back to full-file scan.
        // Covers the catch block in openMultiChromMode().
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "mc_badidx.vcf", HEADER + data);

        // Create a fake "index" file that is not a real tabix index
        Path fakeIndex = tempDir.resolve("mc_badidx.vcf.tbi");
        Files.writeString(fakeIndex, "this is not a tabix index\n");

        VcfInputPartition p = new VcfInputPartition(
                file.toString(), fakeIndex.toString(), new String[]{"chr1"}, conf());

        List<InternalRow> rows = readAll(p);
        // After fallback to full-file scan, the single data record is returned
        assertEquals(1, rows.size());
    }

    // -------------------------------------------------------------------------
    // Split mode — data line encountered before #CHROM header
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Split mode — two sample columns (genotypes map has two entries)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_twoSamples_bothInGenotypesMap() throws Exception {
        String header2 =
                "##fileformat=VCFv4.2\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample1\tsample2\n";
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\tGT\t0/1\t1/1\n";
        Path file = writeVcf(tempDir, "twosamples.vcf", header2 + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertFalse(r.isNullAt(8), "format should not be null");
        assertFalse(r.isNullAt(9), "genotypes map should not be null");
        assertEquals("GT", r.getUTF8String(8).toString());
    }

    // -------------------------------------------------------------------------
    // Split mode — QUAL = "0" maps to 0.0 (not null)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_qualZero_mapsToZeroNotNull() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t0\tPASS\t.\n";
        Path file = writeVcf(tempDir, "qual0.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertFalse(r.isNullAt(5), "QUAL=0 should not be null");
        assertEquals(0.0, r.getDouble(5), 0.0001);
    }

    // -------------------------------------------------------------------------
    // Split mode — INFO with multiple key=value pairs including comma in value
    // -------------------------------------------------------------------------

    @Test
    void splitMode_infoMultipleKeyValues_allPresentInMap() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\tDP=30;AF=0.5;SOMATIC\n";
        Path file = writeVcf(tempDir, "infocomplex.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        // map must be non-null and non-empty (DP, AF, SOMATIC)
        assertFalse(rows.get(0).isNullAt(7));
    }

    // -------------------------------------------------------------------------
    // Split mode — FORMAT present but no sample columns in data line
    // -------------------------------------------------------------------------

    @Test
    void splitMode_formatWithNoSampleData_genotypesNull() throws Exception {
        // Data line has 8 columns only (no FORMAT/genotype values)
        // even though the header declared a sample. Columns 8 and 9 should be null.
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";  // exactly 8 columns
        Path file = writeVcf(tempDir, "nosampledata.vcf", HEADER_WITH_SAMPLE + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(8), "format should be null when data has <9 columns");
        assertTrue(rows.get(0).isNullAt(9), "genotypes should be null when data has <9 columns");
    }

    // -------------------------------------------------------------------------
    // Split mode — FILTER with multiple values (semicolon-separated)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_filterWithValue_storedAsString() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\tLowQual\t.\n";
        Path file = writeVcf(tempDir, "filter.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));

        assertEquals(1, rows.size());
        assertEquals("LowQual", rows.get(0).getUTF8String(6).toString());
    }

    // -------------------------------------------------------------------------
    // Split mode — next() called multiple times after exhaustion returns false
    // -------------------------------------------------------------------------

    @Test
    void splitMode_nextAfterExhaustion_returnsFalse() throws Exception {
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "exhaust.vcf", HEADER + data);

        VcfPartitionReader r = new VcfPartitionReader(splitFull(file));
        try {
            assertTrue(r.next());
            assertFalse(r.next()); // first exhaustion
            assertFalse(r.next()); // second call — must not throw
        } finally {
            r.close();
        }
    }

    // -------------------------------------------------------------------------
    // Single-chrom tabix region query (VCFFileReader + real index)
    // -------------------------------------------------------------------------

    @Test
    void tabixQuery_singleChrom_returnsCorrectCount() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path  = fx.bgzVcf().toString();
        String index = fx.tbiIndex().toString();

        // Region query for chr1 only
        VcfInputPartition p = new VcfInputPartition(
                path, index, "chr1", 1, Integer.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(VcfTestGenerator.VCF_CHR1_COUNT, rows.size());
        for (InternalRow r : rows) {
            assertEquals("chr1", r.getUTF8String(0).toString());
        }
    }

    @Test
    void tabixQuery_singleChrom_posRange_returnsSubset() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path  = fx.bgzVcf().toString();
        String index = fx.tbiIndex().toString();

        // chr1 records: pos 100, 500, 1000 — query [500, MAX_VALUE] should return 2
        VcfInputPartition p = new VcfInputPartition(
                path, index, "chr1", 500, Integer.MAX_VALUE, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(VcfTestGenerator.VCF_CHR1_FROM_500, rows.size());
        for (InternalRow r : rows) {
            assertEquals("chr1", r.getUTF8String(0).toString());
            assertTrue(r.getInt(1) >= 500, "pos should be >= 500");
        }
    }

    @Test
    void tabixQuery_unknownChrom_returnsEmpty() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path  = fx.bgzVcf().toString();
        String index = fx.tbiIndex().toString();

        VcfInputPartition p = new VcfInputPartition(
                path, index, "chrX", 1, Integer.MAX_VALUE, conf());

        assertEquals(0, readAll(p).size());
    }

    @Test
    void tabixQuery_badIndex_fallsBackToFullScan() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path = fx.bgzVcf().toString();

        // Write a non-tabix file as the index to trigger the catch-and-fallback path
        Path fakeIndex = tempDir.resolve("fake.vcf.gz.tbi");
        Files.writeString(fakeIndex, "not a real tabix index\n");

        VcfInputPartition p = new VcfInputPartition(
                path, fakeIndex.toString(), "chr1", 1, Integer.MAX_VALUE, conf());

        // Fallback full-file scan returns all variants (not just chr1)
        List<InternalRow> rows = readAll(p);
        assertEquals(VcfTestGenerator.VCF_TOTAL, rows.size());
    }

    @Test
    void tabixQuery_close_beforeNext_doesNotThrow() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        VcfPartitionReader reader = new VcfPartitionReader(
                new VcfInputPartition(fx.bgzVcf().toString(), fx.tbiIndex().toString(),
                        "chr1", 1, Integer.MAX_VALUE, conf()));
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Multi-chrom tabix mode — nextMultiChromMode() with real index
    // -------------------------------------------------------------------------

    @Test
    void multiChromMode_withIndex_bothChromsReturned() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path  = fx.bgzVcf().toString();
        String index = fx.tbiIndex().toString();

        // One partition covering both chromosomes via multi-chrom tabix iteration
        VcfInputPartition p = new VcfInputPartition(
                path, index, new String[]{"chr1", "chr2"}, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(VcfTestGenerator.VCF_TOTAL, rows.size());
        long chr1Count = rows.stream()
                .filter(r -> "chr1".equals(r.getUTF8String(0).toString()))
                .count();
        long chr2Count = rows.stream()
                .filter(r -> "chr2".equals(r.getUTF8String(0).toString()))
                .count();
        assertEquals(VcfTestGenerator.VCF_CHR1_COUNT, chr1Count);
        assertEquals(VcfTestGenerator.VCF_CHR2_COUNT, chr2Count);
    }

    @Test
    void multiChromMode_withIndex_singleChromArray_returnsOnlyThatChrom() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        String path  = fx.bgzVcf().toString();
        String index = fx.tbiIndex().toString();

        VcfInputPartition p = new VcfInputPartition(
                path, index, new String[]{"chr2"}, conf());

        List<InternalRow> rows = readAll(p);

        assertEquals(VcfTestGenerator.VCF_CHR2_COUNT, rows.size());
        for (InternalRow r : rows) {
            assertEquals("chr2", r.getUTF8String(0).toString());
        }
    }

    @Test
    void multiChromMode_withIndex_unknownChrom_returnsEmpty() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        VcfInputPartition p = new VcfInputPartition(
                fx.bgzVcf().toString(), fx.tbiIndex().toString(),
                new String[]{"chrX"}, conf());

        assertEquals(0, readAll(p).size());
    }

    @Test
    void multiChromMode_withIndex_close_beforeNext_doesNotThrow() throws Exception {
        VcfTestGenerator.Fixtures fx = VcfTestGenerator.generate(tempDir);
        VcfPartitionReader reader = new VcfPartitionReader(
                new VcfInputPartition(fx.bgzVcf().toString(), fx.tbiIndex().toString(),
                        new String[]{"chr1", "chr2"}, conf()));
        assertDoesNotThrow(reader::close);
    }

    // -------------------------------------------------------------------------
    // Split mode — data line before #CHROM header
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // VCFFileReader path — non-PASS filter (specific filter name)
    // exercises the `!vc.getFilters().isEmpty()` branch in getFromVariantContext
    // -------------------------------------------------------------------------

    @Test
    void vcfFileReader_nonPassFilter_storedAsSemicolonJoined() throws Exception {
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##FILTER=<ID=LowQual,Description=\"Low quality\">\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\tT\t30\tLowQual\t.\n";
        Path file = writeVcf(tempDir, "lowqual.vcf", vcf);

        List<InternalRow> rows = readAll(new VcfInputPartition(file.toString(), null, conf()));

        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(6), "specific filter should not be null");
        assertEquals("LowQual", rows.get(0).getUTF8String(6).toString());
    }

    // -------------------------------------------------------------------------
    // VCFFileReader path — encodeInfoValue(Boolean) via Flag INFO field
    // exercises the `value instanceof Boolean` branch
    // -------------------------------------------------------------------------

    @Test
    void vcfFileReader_infoFlagField_encodedAsTrue() throws Exception {
        // htsjdk stores Flag-type INFO fields as Boolean.TRUE in the attributes map.
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description=\"Somatic mutation\">\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\tT\t.\t.\tSOMATIC\n";
        Path file = writeVcf(tempDir, "flaginfo.vcf", vcf);

        List<InternalRow> rows = readAll(new VcfInputPartition(file.toString(), null, conf()));

        assertEquals(1, rows.size());
        // The INFO map must be present; its SOMATIC entry encodes Boolean.TRUE → "true".
        assertFalse(rows.get(0).isNullAt(7), "INFO map should not be null");
    }

    // -------------------------------------------------------------------------
    // VCFFileReader path — encodeInfoValue(List<?>) via multi-value INFO field
    // exercises the `value instanceof List<?>` branch
    // -------------------------------------------------------------------------

    @Test
    void vcfFileReader_infoListField_commaJoined() throws Exception {
        // AF=0.5,0.3 with Number=A is stored by htsjdk as a List<Float>.
        // encodeInfoValue joins the list elements with commas.
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele frequency\">\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n" +
                "chr1\t100\t.\tA\tT,G\t.\t.\tAF=0.5,0.3\n";
        Path file = writeVcf(tempDir, "listinfo.vcf", vcf);

        List<InternalRow> rows = readAll(new VcfInputPartition(file.toString(), null, conf()));

        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(7), "INFO map should not be null for multi-value field");
    }

    // -------------------------------------------------------------------------
    // Split mode — data line before #CHROM header
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Split mode — CRLF line endings stripped correctly
    // (covers the `b != '\r'` = false branch in readLineFromStream)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_crlfLineEndings_strippedCorrectly() throws Exception {
        // Write a VCF file with CRLF (\r\n) endings using raw bytes.
        // readLineFromStream must strip the \r before returning each line.
        String header = "##fileformat=VCFv4.2\r\n" +
                        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\r\n";
        String line1  = "chr1\t100\t.\tA\tT\t.\t.\t.\r\n";
        String line2  = "chr2\t200\t.\tG\tC\t.\t.\t.\r\n";
        Path file = tempDir.resolve("crlf.vcf");
        Files.write(file, (header + line1 + line2).getBytes(StandardCharsets.UTF_8));

        long len = Files.size(file);
        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, len + 1, conf());

        List<InternalRow> rows = readAll(p);
        assertEquals(2, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals(100,    rows.get(0).getInt(1));
        assertEquals("chr2", rows.get(1).getUTF8String(0).toString());
        assertEquals(200,    rows.get(1).getInt(1));
    }

    @Test
    void splitMode_dataLineBeforeChromHeader_readsRemainingData() throws Exception {
        // A VCF-like file where the first line is a data line (not a # comment),
        // so the header scanner hits the break at the "not a comment" guard (line ~284).
        // After the break, fsIn is positioned after the first data line; for startByte==0
        // the reader continues reading from the current position.
        String line1 = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        String line2 = "chr2\t200\t.\tG\tC\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "noheader.vcf", line1 + line2);

        long len = Files.size(file);
        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, len + 1, conf());

        List<InternalRow> rows = readAll(p);
        // The header scanner consumes line1 to look for #CHROM, then breaks.
        // nextSplitMode() reads line2 as the first (and only) data record.
        assertEquals(1, rows.size());
        assertEquals("chr2", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // Split mode — empty string id/alt/qual/filter (not ".", but "")
    // Covers the isEmpty() true branch in getSplitMode()
    // -------------------------------------------------------------------------

    @Test
    void splitMode_emptyStringId_isNull() throws Exception {
        // id = "" (empty, not ".") → isEmpty() = true → null
        String data = "chr1\t100\t\tA\tT\t30\tPASS\tDP=1\n";
        Path file = writeVcf(tempDir, "emptyid.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(2), "empty id should be null");
    }

    @Test
    void splitMode_emptyStringAlt_isNull() throws Exception {
        // alt = "" (empty, not ".") → isEmpty() = true → null
        String data = "chr1\t100\t.\tA\t\t30\tPASS\tDP=1\n";
        Path file = writeVcf(tempDir, "emptyalt.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(4), "empty alt should be null");
    }

    @Test
    void splitMode_emptyStringQual_isNull() throws Exception {
        // qual = "" (empty, not ".") → qualStr.isEmpty() = true → qual stays null
        String data = "chr1\t100\t.\tA\tT\t\tPASS\tDP=1\n";
        Path file = writeVcf(tempDir, "emptyqual.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(5), "empty qual should be null");
    }

    @Test
    void splitMode_emptyStringFilter_isNull() throws Exception {
        // filter = "" (empty, not ".") → filterStr.isEmpty() = true → filter stays null
        String data = "chr1\t100\t.\tA\tT\t30\t\tDP=1\n";
        Path file = writeVcf(tempDir, "emptyfilter.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(6), "empty filter should be null");
    }

    // -------------------------------------------------------------------------
    // Split mode — format column present but empty (c[8].trim().isEmpty())
    // -------------------------------------------------------------------------

    @Test
    void splitMode_emptyFormatColumn_formatAndGenotypesNull() throws Exception {
        // 9 columns, col 8 = empty string → c[8].trim().isEmpty() = true
        // → format and genotypes stay null
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\t\n";
        Path file = writeVcf(tempDir, "emptyformat.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).isNullAt(8), "format should be null for empty col 8");
        assertTrue(rows.get(0).isNullAt(9), "genotypes should be null for empty col 8");
    }

    // -------------------------------------------------------------------------
    // openSplitMode — lowercase "#chrom" header line
    // Covers the startsWith("#chrom") branch
    // -------------------------------------------------------------------------

    @Test
    void openSplitMode_lowercaseChromHeader_parsedCorrectly() throws Exception {
        // Header uses lowercase "#chrom" instead of "#CHROM" — triggers the
        // line.startsWith("#chrom") branch in the header-scan loop.
        String header = "##fileformat=VCFv4.2\n" +
                "#chrom\tpos\tid\tref\talt\tqual\tfilter\tinfo\n";
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\n";
        Path file = writeVcf(tempDir, "lowercase.vcf", header + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
    }

    // -------------------------------------------------------------------------
    // VCFFileReader path — buildGenotypesMap with extended FORMAT fields (DP)
    // Covers the extKeys iteration and val != null branch
    // -------------------------------------------------------------------------

    @Test
    void vcfFileReader_genotypesWithExtendedAttrs_encodedCorrectly() throws Exception {
        // FORMAT=GT:MYATTR uses a non-standard key (not GT/GQ/DP/AD/PL) so htsjdk
        // stores it in getExtendedAttributes().  Two samples: sample1 has MYATTR=42
        // (val != null → "42") and sample2 has MYATTR=. (htsjdk converts Integer "."
        // to null → val == null → ".").  Covers the extKeys loop body, the
        // val != null → true branch, and the val != null → false branch in
        // buildGenotypesMap().
        String vcf =
                "##fileformat=VCFv4.2\n" +
                "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n" +
                "##FORMAT=<ID=MYATTR,Number=1,Type=Integer,Description=\"Custom attr\">\n" +
                "##contig=<ID=chr1,length=1000000>\n" +
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample1\tsample2\n" +
                "chr1\t100\t.\tA\tT\t.\t.\t.\tGT:MYATTR\t0/1:42\t1/1:.\n";
        Path file = writeVcf(tempDir, "extattrs.vcf", vcf);

        List<InternalRow> rows = readAll(new VcfInputPartition(file.toString(), null, conf()));
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(8), "format should not be null");
        assertFalse(rows.get(0).isNullAt(9), "genotypes map should not be null");
        assertTrue(rows.get(0).getUTF8String(8).toString().contains("MYATTR"),
                "format string should include non-standard attribute MYATTR");
    }

    // -------------------------------------------------------------------------
    // Split mode — FORMAT present but no sample declared in header
    // (sampleNames.length == 0 but c.length > 9)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_formatPresentNoSamplesInHeader_genotypesNull() throws Exception {
        // Header has no sample columns; data line has FORMAT + one genotype column.
        // sampleNames.length == 0 → genotypesMap stays null even though c.length > 9.
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\tGT\t0/1\n";
        Path file = writeVcf(tempDir, "nosampledecl.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        InternalRow r = rows.get(0);
        assertFalse(r.isNullAt(8), "format should not be null (col 8 has 'GT')");
        assertTrue(r.isNullAt(9), "genotypes should be null when no samples in header");
    }

    // -------------------------------------------------------------------------
    // parseInfoString — empty pair from double semicolon (pair.isEmpty() → skip)
    // -------------------------------------------------------------------------

    @Test
    void splitMode_infoDoubleSemicolon_emptyPairSkipped() throws Exception {
        // INFO = "DP=30;;AF=0.5": splitting on ";" produces ["DP=30", "", "AF=0.5"].
        // The empty string triggers pair.isEmpty() = true → continue (skipped).
        // The result map contains DP and AF but not the empty entry.
        String data = "chr1\t100\t.\tA\tT\t.\t.\tDP=30;;AF=0.5\n";
        Path file = writeVcf(tempDir, "doublesemi.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(7), "INFO map should not be null");
    }

    // -------------------------------------------------------------------------
    // openSplitMode header scan — readLineFromStream returns null at EOF
    // (file has only ##meta lines, no #CHROM, no data lines)
    // -------------------------------------------------------------------------

    @Test
    void openSplitMode_eoFDuringHeaderScan_noDataReturned() throws Exception {
        // File has only ##meta lines with no #CHROM line.
        // The header-scan while loop hits readLineFromStream() == null (EOF with
        // empty sb → returns null) → loop exits.  nextSplitMode() then calls
        // readLineFromStream() again → null → returns false → 0 records.
        String content = "##fileformat=VCFv4.2\n##source=test\n";
        Path file = tempDir.resolve("metaonly.vcf");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        long len = Files.size(file);
        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, len + 1, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // openSplitMode — empty file: readLineFromStream returns null immediately
    // (sb.length() == 0 at EOF → returns null, not empty string)
    // -------------------------------------------------------------------------

    @Test
    void openSplitMode_emptyFile_returnsNoRecords() throws Exception {
        // An empty file: readLineFromStream(fsIn) returns null on the very first
        // call (stream.read() = -1, sb.length() = 0 → null).  The while condition
        // in openSplitMode is immediately false; nextSplitMode then returns false.
        Path file = tempDir.resolve("empty_split.vcf");
        Files.writeString(file, "", StandardCharsets.UTF_8);

        // endByte = 1 → split mode; startByte = 0 so no past-EOF early return
        VcfInputPartition p = new VcfInputPartition(
                file.toString(), null, null, 1, Integer.MAX_VALUE,
                0L, 1L, conf());

        assertEquals(0, readAll(p).size());
    }

    // -------------------------------------------------------------------------
    // readLineFromStream — sb.length() > 0 → true path (file without trailing \n)
    // When the last data line has no '\n', readLineFromStream() hits EOF with a
    // non-empty sb and returns sb.toString() rather than null.
    // -------------------------------------------------------------------------

    @Test
    void splitMode_noTrailingNewline_lastLineReturned() throws Exception {
        // Last data line has no trailing '\n': readLineFromStream() accumulates the
        // line content into sb, then hits EOF (b == -1) → sb.length() > 0 → returns
        // sb.toString() rather than null.  The record must still be returned.
        String content = HEADER + "chr1\t100\t.\tA\tT\t.\t.\t.";  // no trailing newline
        Path file = tempDir.resolve("notrailingnl.vcf");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertEquals("chr1", rows.get(0).getUTF8String(0).toString());
        assertEquals(100,    rows.get(0).getInt(1));
    }

    // -------------------------------------------------------------------------
    // getSplitMode — sampleNames.length > 0 && c.length > 9: false when c.length == 9
    // FORMAT present (col 8 non-empty) but data line has only 9 columns (no genotype
    // values) even though the header declared a sample.
    // -------------------------------------------------------------------------

    @Test
    void splitMode_sampleDeclaredButDataHas9Cols_genotypesNull() throws Exception {
        // Header declares sample1; data line has exactly 9 columns (FORMAT col present
        // but no sample-value col).  sampleNames.length > 0 is true but c.length > 9
        // is false → genotypesMap stays null even though FORMAT is populated.
        String data = "chr1\t100\t.\tA\tT\t.\t.\t.\tGT\n"; // 9 columns, no sample value
        Path file = writeVcf(tempDir, "nogenocol.vcf", HEADER_WITH_SAMPLE + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        assertFalse(rows.get(0).isNullAt(8), "format col (GT) should not be null");
        assertTrue(rows.get(0).isNullAt(9),
                "genotypes should be null when data line has no sample-value column");
    }

    // -------------------------------------------------------------------------
    // parseInfoString — trimmed.isEmpty() = true (INFO is empty string, not ".")
    // When INFO column is "" (empty after trim), the || trimmed.isEmpty() branch is
    // taken (the ".".equals branch is false since "" != ".").
    // -------------------------------------------------------------------------

    @Test
    void splitMode_emptyStringInfo_returnsEmptyMap() throws Exception {
        // INFO = "" (empty, not ".") → ".".equals("") = false, "".isEmpty() = true
        // → takes the || second operand branch → returns empty map.
        String data = "chr1\t100\t.\tA\tT\t.\t.\t\n"; // col 7 is empty string
        Path file = writeVcf(tempDir, "emptystrinfo.vcf", HEADER + data);

        List<InternalRow> rows = readAll(splitFull(file));
        assertEquals(1, rows.size());
        // Empty INFO still produces a non-null (empty) map
        assertFalse(rows.get(0).isNullAt(7), "empty-string INFO should yield non-null empty map");
    }
}
