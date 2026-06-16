package io.github.peterdowdy.litebfx.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests against the real-world {@code range.bam} from the htslib test suite.
 *
 * <p>Source: <a href="https://github.com/samtools/htslib/blob/develop/test/range.bam">
 * samtools/htslib test/range.bam</a>
 *
 * <h3>Ground truth (verified with samtools 1.21)</h3>
 * <pre>
 * samtools view -c range.bam               → 112
 * samtools view -c range.bam CHROMOSOME_I  → 18
 * samtools view -c range.bam CHROMOSOME_II → 34
 * samtools view -c range.bam CHROMOSOME_III→ 41
 * samtools view -c range.bam CHROMOSOME_IV → 19
 *
 * First record (samtools view range.bam | head -1):
 *   HS18_09653:4:1315:19857:61712  145  CHROMOSOME_I  914  23  78M1D22M
 *   CHROMOSOME_V  1104758  0
 *   AGCTAGGGCACTTTTTGTCTGCCCAAATATAGGCAACCAAAAATAATTTCCAAGTTTTTAATGATTTGTTGCATATTGAAAAAACATTTTTTGGGTTTTT
 *   GGGFGHFDFG@IHG?DFGBFCGGHEEEHFFFGFCFFHIHGCHGHHIGIFHIGFHHIDH=GFADFGHHIHGEGIDGFFDGEGHFHFGFEEGFFFFEDDD<?
 *   ... NM:i:3  RG:Z:1
 * </pre>
 *
 * Data: C. elegans Illumina paired-end alignments (ERS225193), 7 reference sequences.
 */
class RangeBamTest {

    // Ground truth from samtools
    static final int TOTAL_RECORDS       = 112;
    static final int CHROMOSOME_I_COUNT  = 18;
    static final int CHROMOSOME_II_COUNT = 34;
    static final int CHROMOSOME_III_COUNT = 41;
    static final int CHROMOSOME_IV_COUNT = 19;

    static String bamUri;

    @BeforeAll
    static void findBamResource() throws Exception {
        URL url = RangeBamTest.class.getClassLoader().getResource("range.bam");
        assertNotNull(url, "range.bam not found in test resources");
        bamUri = Paths.get(url.toURI()).toUri().toString();
    }

    // -------------------------------------------------------------------------
    // Count tests
    // -------------------------------------------------------------------------

    @Test
    void totalRecordCount_matches_samtools() throws IOException {
        assertEquals(TOTAL_RECORDS, readAll().size());
    }

    @Test
    void recordCountPerChromosome_matches_samtools() throws IOException {
        List<InternalRow> rows = readAll();

        int chrI = 0, chrII = 0, chrIII = 0, chrIV = 0;
        for (InternalRow row : rows) {
            UTF8String ref = row.getUTF8String(2); // referenceName
            if (ref == null) continue;
            switch (ref.toString()) {
                case "CHROMOSOME_I"   -> chrI++;
                case "CHROMOSOME_II"  -> chrII++;
                case "CHROMOSOME_III" -> chrIII++;
                case "CHROMOSOME_IV"  -> chrIV++;
            }
        }

        assertEquals(CHROMOSOME_I_COUNT,   chrI,   "CHROMOSOME_I count");
        assertEquals(CHROMOSOME_II_COUNT,  chrII,  "CHROMOSOME_II count");
        assertEquals(CHROMOSOME_III_COUNT, chrIII, "CHROMOSOME_III count");
        assertEquals(CHROMOSOME_IV_COUNT,  chrIV,  "CHROMOSOME_IV count");
    }

    // -------------------------------------------------------------------------
    // First record — verified field-by-field against samtools
    // -------------------------------------------------------------------------

    @Test
    void firstRecord_readName() throws IOException {
        assertEquals("HS18_09653:4:1315:19857:61712", firstRow().getUTF8String(0).toString());
    }

    @Test
    void firstRecord_flags_145() throws IOException {
        assertEquals(145, firstRow().getInt(1));
    }

    @Test
    void firstRecord_referenceNameAndStart() throws IOException {
        InternalRow row = firstRow();
        assertEquals("CHROMOSOME_I", row.getUTF8String(2).toString());
        assertEquals(914L, row.getLong(3)); // 1-based, LongType
    }

    @Test
    void firstRecord_mappingQualityAndCigar() throws IOException {
        InternalRow row = firstRow();
        assertEquals(23,         row.getInt(4));
        assertEquals("78M1D22M", row.getUTF8String(5).toString());
    }

    @Test
    void firstRecord_mateFields_crossChromosome() throws IOException {
        // Mate is mapped to CHROMOSOME_V (different chromosome), insertSize = 0
        InternalRow row = firstRow();
        assertEquals("CHROMOSOME_V", row.getUTF8String(6).toString()); // mateReferenceName
        assertEquals(1104758L,       row.getLong(7));                   // mateStart (LongType)
        assertEquals(0,              row.getInt(8));                    // insertSize
    }

    @Test
    void firstRecord_sequenceAndQuality() throws IOException {
        InternalRow row = firstRow();
        assertEquals(
            "AGCTAGGGCACTTTTTGTCTGCCCAAATATAGGCAACCAAAAATAATTTCCAAGTTTTTAAT" +
            "GATTTGTTGCATATTGAAAAAACATTTTTTGGGTTTTT",
            row.getUTF8String(9).toString());
        assertEquals(
            "GGGFGHFDFG@IHG?DFGBFCGGHEEEHFFFGFCFFHIHGCHGHHIGIFHIGFHHIDH=GF" +
            "ADFGHHIHGEGIDGFFDGEGHFHFGFEEGFFFFEDDD<?",
            row.getUTF8String(10).toString());
    }

    @Test
    void firstRecord_attributes_nmAndRg() throws IOException {
        InternalRow row = firstRow();
        assertFalse(row.isNullAt(11));
        Map<String, String> attrs = toJavaMap(row.getMap(11));
        assertEquals("3", attrs.get("NM"), "NM tag");
        assertEquals("1", attrs.get("RG"), "RG tag");
    }

    // -------------------------------------------------------------------------
    // CIGAR and structural variety
    // -------------------------------------------------------------------------

    @Test
    void cigars_includeVariousOperations() throws IOException {
        // Verify we see deletions (D), soft-clips (S), and plain matches (M)
        Set<String> cigars = new HashSet<>();
        for (InternalRow row : readAll()) {
            UTF8String c = row.getUTF8String(5);
            if (c != null) cigars.add(c.toString());
        }
        assertTrue(cigars.stream().anyMatch(c -> c.contains("D")), "expect deletion CIGAR ops");
        assertTrue(cigars.stream().anyMatch(c -> c.contains("S")), "expect soft-clip CIGAR ops");
        assertTrue(cigars.stream().anyMatch(c -> c.equals("100M")), "expect 100M");
    }

    @Test
    void allRecords_haveNonNullReadNameAndReferenceName() throws IOException {
        for (InternalRow row : readAll()) {
            assertNotNull(row.getUTF8String(0), "readName should not be null");
            assertNotNull(row.getUTF8String(2), "referenceName should not be null (all reads are mapped)");
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<InternalRow> readAll() throws IOException {
        BamInputPartition partition = BamInputPartition.forFullScan(
            bamUri, new Configuration());
        List<InternalRow> rows = new ArrayList<>();
        try (BamPartitionReader reader = new BamPartitionReader(partition, true)) {
            while (reader.next()) rows.add(reader.get());
        }
        return rows;
    }

    private static InternalRow firstRow() throws IOException {
        return readAll().get(0);
    }

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
