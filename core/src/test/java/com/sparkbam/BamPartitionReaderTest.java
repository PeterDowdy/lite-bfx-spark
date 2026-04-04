package com.litebfx;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
    // Error case
    // -------------------------------------------------------------------------

    @Test
    void nonZeroStartOffset_throwsUnsupportedOperationException() {
        BamInputPartition partition = new BamInputPartition(
            fixtures.bam().toUri().toString(),
            /* startVFO */ 1024L,
            /* endVFO   */ Long.MAX_VALUE,
            new Configuration());

        BamPartitionReader reader = new BamPartitionReader(partition, false);
        assertThrows(UnsupportedOperationException.class, reader::next);
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
}
