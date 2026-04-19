package com.litebfx.bam;

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
        BamInputPartition partition = new BamInputPartition(
            fixtures.bam().toUri().toString(),
            0L, Long.MAX_VALUE,
            new Configuration(),
            /* indexPath */ null,
            false, null, "none",
            /* querySequence */ null, 1, Integer.MAX_VALUE,
            /* querySequences */ new String[]{TestBamGenerator.REF_NAME},
            /* queryUnmapped  */ false);

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
        BamInputPartition partition = new BamInputPartition(
            fixtures.bam().toUri().toString(),
            0L, Long.MAX_VALUE,
            new Configuration(),
            /* indexPath */ baiUri,
            false, null, "none",
            /* querySequence */ null, 1, Integer.MAX_VALUE,
            /* querySequences */ new String[]{TestBamGenerator.REF_NAME},
            /* queryUnmapped  */ false);

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
        BamInputPartition partition = new BamInputPartition(
            fixtures.bam().toUri().toString(),
            0L, Long.MAX_VALUE,
            new Configuration(),
            /* indexPath */ baiUri,
            false, null, "none",
            /* querySequence */ null, 1, Integer.MAX_VALUE,
            /* querySequences */ null,
            /* queryUnmapped  */ true);

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
    // Helpers
    // -------------------------------------------------------------------------

    /** Opens a full-file partition for the given path and collects all rows. */
    private static List<InternalRow> readAll(Path path, boolean includeAttributes)
        throws IOException {
        BamInputPartition partition = new BamInputPartition(
            path.toUri().toString(),
            /* startVFO */ 0L,
            /* endVFO   */ Long.MAX_VALUE,
            new Configuration());

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
        BamInputPartition partition = new BamInputPartition(
            bamPath.toUri().toString(), startByte, endByte, new Configuration());
        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, false)) {
            while (reader.next()) rows.add(reader.get());
        }
        return rows;
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
