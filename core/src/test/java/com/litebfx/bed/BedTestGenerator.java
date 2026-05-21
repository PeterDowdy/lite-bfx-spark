package com.litebfx.bed;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.bed.BEDCodec;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

/**
 * Generates deterministic BED test fixtures for use in {@link BedDataSourceTest}.
 *
 * <p>Produces:
 * <ul>
 *   <li>A bgzip-compressed BED6 file ({@code test.bed.gz}) with a tabix index ({@code test.bed.gz.tbi}),
 *       containing records on {@code chr1} (3 records) and {@code chr2} (2 records).</li>
 *   <li>A plain (uncompressed) BED3 file ({@code test3.bed}) with 2 records (no extra fields).</li>
 *   <li>A bgzip-compressed BED12 file ({@code test12.bed.gz}) with 1 record.</li>
 * </ul>
 */
public class BedTestGenerator {

    /** Number of records in the BED6 bgzip fixture (chr1 + chr2). */
    public static final int BED6_TOTAL = 5;
    /** Records on chr1 in the BED6 fixture. */
    public static final int BED6_CHR1_COUNT = 3;
    /** Records on chr2 in the BED6 fixture. */
    public static final int BED6_CHR2_COUNT = 2;
    /** Records in the BED6 fixture with chromStart >= 500 on chr1. */
    public static final int BED6_CHR1_FROM_500 = 2;

    public static final int BED3_TOTAL  = 2;
    public static final int BED12_TOTAL = 1;

    public record Fixtures(
        java.net.URI bed6Bgzf,
        java.net.URI bed6Tbi,
        java.net.URI bed3Plain,
        java.net.URI bed12Bgzf
    ) {}

    public static Fixtures generate(Path tempDir) throws IOException {
        Path bed6Path   = tempDir.resolve("test.bed.gz");
        Path tbiPath    = tempDir.resolve("test.bed.gz.tbi");
        Path bed3Path   = tempDir.resolve("test3.bed");
        Path bed12Path  = tempDir.resolve("test12.bed.gz");

        // --- BED6 bgzip ---
        try (BlockCompressedOutputStream out =
                 new BlockCompressedOutputStream(bed6Path.toFile())) {
            PrintWriter pw = new PrintWriter(out);
            // chr1 records (sorted by start)
            pw.println("chr1\t100\t200\tpeak1\t500\t+");
            pw.println("chr1\t500\t700\tpeak2\t800\t-");
            pw.println("chr1\t1000\t1200\tpeak3\t600\t.");
            // chr2 records
            pw.println("chr2\t100\t300\tpeak4\t700\t+");
            pw.println("chr2\t500\t600\tpeak5\t900\t-");
            pw.flush();
        }

        // Create tabix index
        TabixIndex tbi = IndexFactory.createTabixIndex(bed6Path.toFile(), new BEDCodec(), null);
        tbi.write(tbiPath);

        // --- BED3 plain ---
        try (PrintWriter pw = new PrintWriter(bed3Path.toFile())) {
            pw.println("chr1\t100\t200");
            pw.println("chr1\t500\t700");
        }

        // --- BED12 bgzip ---
        try (BlockCompressedOutputStream out =
                 new BlockCompressedOutputStream(bed12Path.toFile())) {
            PrintWriter pw = new PrintWriter(out);
            // BED12: chrom chromStart chromEnd name score strand thickStart thickEnd rgb blockCount blockSizes blockStarts
            pw.println("chr1\t0\t1000\tfeat1\t0\t+\t100\t900\t255,0,0\t3\t100,200,50\t0,300,650");
            pw.flush();
        }

        return new Fixtures(
                bed6Path.toUri(),
                tbiPath.toUri(),
                bed3Path.toUri(),
                bed12Path.toUri()
        );
    }
}
