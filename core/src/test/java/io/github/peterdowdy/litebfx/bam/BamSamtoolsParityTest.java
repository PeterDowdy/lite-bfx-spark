package io.github.peterdowdy.litebfx.bam;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * samtools parity: the BAM reader must reproduce {@code samtools view} exactly.
 *
 * <p>{@code samtools} is the reference implementation and an entirely independent code path
 * from htsjdk, so this is the strongest available value oracle. Each record is reduced to a
 * canonical signature (all SAM columns + sorted optional tags, with {@code *}/{@code =}/{@code 0}
 * normalized) and the two multisets are compared. Skipped when {@code samtools} is not on PATH.
 */
class BamSamtoolsParityTest {

    static SparkSession spark;
    static String bamUri;    // file:// URI for Spark
    static String bamFile;   // plain filesystem path for samtools

    @BeforeAll
    static void setUp() throws Exception {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("BamSamtoolsParityTest")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
        URL u = BamSamtoolsParityTest.class.getClassLoader().getResource("range.bam");
        assertNotNull(u, "range.bam not found in test resources");
        bamUri = Paths.get(u.toURI()).toUri().toString();
        bamFile = Paths.get(u.toURI()).toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    @Test
    void bamRead_matchesSamtoolsView() throws Exception {
        List<String> samLines = runSamtoolsView(bamFile, null);
        assumeTrue(samLines != null, "samtools not available on PATH");

        List<String> expected = new ArrayList<>();
        for (String line : samLines) expected.add(sigFromSam(line));
        Collections.sort(expected);

        List<Row> rows = spark.read().format("bam").load(bamUri).collectAsList();
        List<String> actual = new ArrayList<>();
        for (Row r : rows) actual.add(sigFromRow(r));
        Collections.sort(actual);

        assertEquals(expected, actual, "BAM reader output must match samtools view");
    }

    @Test
    void bamRegion_matchesSamtoolsView() throws Exception {
        List<String> samLines = runSamtoolsView(bamFile, "CHROMOSOME_I:1-2000");
        assumeTrue(samLines != null, "samtools not available on PATH");

        List<String> expected = new ArrayList<>();
        for (String line : samLines) expected.add(sigFromSam(line));
        Collections.sort(expected);

        List<Row> rows = spark.read().format("bam").load(bamUri)
                .filter("referenceName = 'CHROMOSOME_I' AND start >= 1 AND start <= 2000")
                .collectAsList();
        List<String> actual = new ArrayList<>();
        for (Row r : rows) actual.add(sigFromRow(r));
        Collections.sort(actual);

        assertEquals(expected, actual, "region read must match samtools view region query");
    }

    // --- canonical signatures ---------------------------------------------

    private static String sigFromSam(String line) {
        String[] f = line.split("\t", -1);
        String rnext = "=".equals(f[6]) ? f[2] : f[6];
        List<String> tags = new ArrayList<>();
        for (int i = 11; i < f.length; i++) tags.add(f[i]);   // TAG:TYPE:VALUE
        Collections.sort(tags);
        return String.join("|", f[0], f[1], f[2], f[3], f[4], f[5], rnext, f[7], f[8],
                f[9], f[10], String.join(",", tags));
    }

    private static String sigFromRow(Row r) {
        String rname = r.isNullAt(2) ? "*" : r.getString(2);
        String pos   = r.isNullAt(3) ? "0" : String.valueOf(r.getLong(3));
        String mapq  = r.isNullAt(4) ? "0" : String.valueOf(r.getInt(4));
        String cigar = r.isNullAt(5) ? "*" : r.getString(5);
        String rnext = r.isNullAt(6) ? "*" : r.getString(6);
        String pnext = r.isNullAt(7) ? "0" : String.valueOf(r.getLong(7));
        String tlen  = r.isNullAt(8) ? "0" : String.valueOf(r.getInt(8));
        String seq   = r.isNullAt(9) ? "*" : r.getString(9);
        String qual  = r.isNullAt(10) ? "*" : r.getString(10);
        List<String> tags = new ArrayList<>();
        if (!r.isNullAt(11)) {
            Map<String, String> attrs = r.getJavaMap(11);
            for (Map.Entry<String, String> e : attrs.entrySet()) {
                tags.add(e.getKey() + ":" + e.getValue());     // TAG + TYPE:VALUE
            }
        }
        Collections.sort(tags);
        return String.join("|", r.getString(0), String.valueOf(r.getInt(1)), rname, pos,
                mapq, cigar, rnext, pnext, tlen, seq, qual, String.join(",", tags));
    }

    // --- samtools ---------------------------------------------------------

    private static List<String> runSamtoolsView(String path, String region) {
        List<String> cmd = new ArrayList<>(List.of("samtools", "view", path));
        if (region != null) cmd.add(region);
        try {
            Process p = new ProcessBuilder(cmd).start();
            List<String> lines = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String l;
                while ((l = br.readLine()) != null) {
                    if (!l.isEmpty()) lines.add(l);
                }
            }
            return p.waitFor() == 0 ? lines : null;
        } catch (Exception e) {
            return null;   // samtools not installed / failed -> skip
        }
    }
}
