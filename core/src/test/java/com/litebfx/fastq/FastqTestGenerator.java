package com.litebfx.fastq;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

/**
 * Generates deterministic FASTQ test fixtures for use in {@link FastqDataSourceTest}.
 *
 * <p>Produces:
 * <ul>
 *   <li>A plain uncompressed FASTQ ({@code test.fastq}) with {@value #PLAIN_COUNT} records.</li>
 *   <li>A gzipped FASTQ ({@code test.fastq.gz}) with {@value #GZIP_COUNT} records.</li>
 *   <li>A plain FASTQ ({@code nodesc.fastq}) with no description field in headers
 *       ({@value #NODESC_COUNT} records).</li>
 * </ul>
 *
 * <p>All reads use a fixed 10-base sequence and quality string for simplicity.
 */
public class FastqTestGenerator {

    public static final int PLAIN_COUNT  = 100;
    public static final int GZIP_COUNT   = 50;
    public static final int NODESC_COUNT = 10;

    /** Expected readName of record 0 in the plain fixture. */
    public static final String FIRST_READ_NAME = "read0";
    /** Expected description of record 0 in the plain fixture. */
    public static final String FIRST_DESCRIPTION = "description0";
    /** Expected sequence of all generated records. */
    public static final String SEQUENCE = "ACGTACGTAC";
    /** Expected base quality string of all generated records. */
    public static final String BASE_QUALITIES = "IIIIIIIIII";

    public record Fixtures(
        java.net.URI plainFastq,
        java.net.URI gzipFastq,
        java.net.URI noDescFastq
    ) {}

    public static Fixtures generate(Path tempDir) throws IOException {
        Path plainPath   = tempDir.resolve("test.fastq");
        Path gzipPath    = tempDir.resolve("test.fastq.gz");
        Path noDescPath  = tempDir.resolve("nodesc.fastq");

        writeRecords(java.nio.file.Files.newOutputStream(plainPath), PLAIN_COUNT, true);
        writeRecords(new GZIPOutputStream(java.nio.file.Files.newOutputStream(gzipPath)), GZIP_COUNT, true);
        writeRecords(java.nio.file.Files.newOutputStream(noDescPath), NODESC_COUNT, false);

        return new Fixtures(
                plainPath.toUri(),
                gzipPath.toUri(),
                noDescPath.toUri()
        );
    }

    private static void writeRecords(OutputStream out, int count, boolean withDescription)
            throws IOException {
        try (PrintWriter pw = new PrintWriter(out, true, StandardCharsets.UTF_8)) {
            for (int i = 0; i < count; i++) {
                if (withDescription) {
                    pw.println("@read" + i + " description" + i);
                } else {
                    pw.println("@read" + i);
                }
                pw.println(SEQUENCE);
                pw.println("+");
                pw.println(BASE_QUALITIES);
            }
        }
    }
}
