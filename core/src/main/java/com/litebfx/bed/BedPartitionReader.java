package com.litebfx.bed;

import com.litebfx.HadoopSeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.Block;
import htsjdk.tribble.index.tabix.TabixIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Reads BED records from a single {@link BedInputPartition} and converts them
 * to Spark {@link InternalRow}s matching {@link BedSchema#SCHEMA}.
 *
 * <p>Parses raw tab-separated BED lines directly rather than using BEDCodec, so
 * all 12 standard BED columns (including thickStart/thickEnd and block fields)
 * are available.
 *
 * <p>For bgzip-compressed files with a tabix index and a pushed region, the
 * reader uses {@link TabixIndex#getBlocks} to seek directly to the relevant
 * BGZF chunks.  For full-file scans (no index or no region filter), the file is
 * read sequentially from start to end.
 *
 * <p>Coordinates: BED files use 0-based half-open intervals.  {@code chromStart}
 * and {@code chromEnd} are stored as-is (0-based) in the output schema.
 */
public class BedPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(BedPartitionReader.class);

    private final BedInputPartition partition;

    // Resources opened on first next() call
    private FSDataInputStream fsIn;
    private BlockCompressedInputStream bcis;   // non-null for bgzip (BGZF) files
    private BufferedReader plainReader;         // non-null for plain or regular-gzip files

    // Tabix iteration state
    private List<Block> tabixBlocks;
    private Iterator<Block> blockIterator;
    private long currentBlockEnd = Long.MAX_VALUE;

    private String[] currentColumns;
    private boolean opened = false;

    public BedPartitionReader(BedInputPartition partition) {
        log.trace("BedPartitionReader(path={}, queryChrom={})",
                partition.getPath(), partition.getQueryChrom());
        this.partition = partition;
    }

    // -------------------------------------------------------------------------
    // PartitionReader contract
    // -------------------------------------------------------------------------

    @Override
    public boolean next() throws IOException {
        if (!opened) {
            open();
            opened = true;
        }
        while (true) {
            String line = readLine();
            if (line == null) {
                // If using tabix blocks, try the next block
                if (blockIterator != null && blockIterator.hasNext()) {
                    seekToBlock(blockIterator.next());
                    continue;
                }
                return false;
            }
            // Skip blank lines and header/comment lines
            if (line.isEmpty() || line.startsWith("#")
                    || line.startsWith("track") || line.startsWith("browser")) {
                continue;
            }
            currentColumns = line.split("\t", -1);
            if (currentColumns.length < 3) continue; // malformed
            return true;
        }
    }

    @Override
    public InternalRow get() {
        String[] c = currentColumns;
        int n = c.length;

        Object[] values = new Object[12];
        // 0: chrom
        values[0]  = UTF8String.fromString(c[0]);
        // 1: chromStart (0-based, stored as-is)
        values[1]  = Long.parseLong(c[1].trim());
        // 2: chromEnd (0-based exclusive, stored as-is)
        values[2]  = Long.parseLong(c[2].trim());
        // 3: name — null when absent or "."
        values[3]  = (n > 3 && !".".equals(c[3].trim()))
                ? UTF8String.fromString(c[3].trim()) : null;
        // 4: score — null when absent
        values[4]  = (n > 4) ? parseIntOrNull(c[4]) : null;
        // 5: strand — null when absent or "."
        String strand = (n > 5) ? c[5].trim() : null;
        values[5]  = (strand != null && !strand.isEmpty() && !".".equals(strand))
                ? UTF8String.fromString(strand) : null;
        // 6: thickStart (0-based) — null when absent
        values[6]  = (n > 6) ? parseLongOrNull(c[6]) : null;
        // 7: thickEnd (0-based exclusive) — null when absent
        values[7]  = (n > 7) ? parseLongOrNull(c[7]) : null;
        // 8: itemRgb — null when absent or "0"
        if (n > 8) {
            String rgb = c[8].trim();
            values[8] = ("0".equals(rgb) || rgb.isEmpty())
                    ? null : parseItemRgb(rgb);
        } else {
            values[8] = null;
        }
        // 9: blockCount — null when absent
        values[9]  = (n > 9) ? parseIntOrNull(c[9]) : null;
        // 10: blockSizes — null when absent
        values[10] = (n > 10 && !c[10].trim().isEmpty())
                ? UTF8String.fromString(stripTrailingComma(c[10].trim())) : null;
        // 11: blockStarts — null when absent
        values[11] = (n > 11 && !c[11].trim().isEmpty())
                ? UTF8String.fromString(stripTrailingComma(c[11].trim())) : null;

        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        log.trace("close()");
        try {
            if (bcis != null)        bcis.close();
            if (plainReader != null) plainReader.close();
        } finally {
            if (fsIn != null) fsIn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers — open
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        String path      = partition.getPath();
        String indexPath = partition.getIndexPath();
        String queryChrom = partition.getQueryChrom();
        log.trace("open() path={} indexPath={} queryChrom={}", path, indexPath, queryChrom);

        Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(path);
        FileSystem fs   = hadoopPath.getFileSystem(conf);

        long fileLength = fs.getFileStatus(hadoopPath).getLen();
        fsIn = fs.open(hadoopPath);
        boolean isBgzip = looksCompressed(path) && isBgzfStream(fsIn);

        if (isBgzip) {
            fsIn.seek(0); // reset after header probe
            HadoopSeekableStream seekable = new HadoopSeekableStream(fsIn, fileLength, path);
            bcis = new BlockCompressedInputStream(seekable);

            if (indexPath != null && queryChrom != null) {
                // Load tabix index. TabixIndex(File) wraps in BlockCompressedInputStream
                // internally; TabixIndex(InputStream) does not, so we use File for local paths.
                try {
                    java.io.File idxFile = new java.io.File(new java.net.URI(indexPath));
                    TabixIndex tabix = new TabixIndex(idxFile);
                    // Convert 0-based queryStart to 1-based for tabix
                    int htsjdkStart = (int) Math.min(partition.getQueryStart() + 1, Integer.MAX_VALUE);
                    int htsjdkEnd   = (int) Math.min(partition.getQueryEnd(),       Integer.MAX_VALUE);
                    tabixBlocks = tabix.getBlocks(queryChrom, htsjdkStart, htsjdkEnd);
                    log.trace("open() tabix blocks={}", tabixBlocks.size());
                } catch (java.net.URISyntaxException e) {
                    throw new IOException("Invalid index path URI: " + indexPath, e);
                }
                if (tabixBlocks.isEmpty()) {
                    // No matching blocks — reader will return nothing
                    currentBlockEnd = -1L;
                } else {
                    blockIterator = tabixBlocks.iterator();
                    seekToBlock(blockIterator.next());
                }
            }
            // else: full-file scan from position 0 (default)
        } else if (looksCompressed(path)) {
            // Regular gzip — seek back to 0 before wrapping
            fsIn.seek(0);
            plainReader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(fsIn), StandardCharsets.UTF_8));
        } else {
            // Plain text BED file
            plainReader = new BufferedReader(new InputStreamReader(fsIn, StandardCharsets.UTF_8));
        }
    }

    private void seekToBlock(Block block) throws IOException {
        log.trace("seekToBlock(start={}, end={})", block.getStartPosition(), block.getEndPosition());
        bcis.seek(block.getStartPosition());
        currentBlockEnd = block.getEndPosition();
    }

    // -------------------------------------------------------------------------
    // Internal helpers — read
    // -------------------------------------------------------------------------

    private String readLine() throws IOException {
        if (bcis != null) {
            // Check end-of-block boundary
            if (currentBlockEnd >= 0 && bcis.getPosition() >= currentBlockEnd) {
                return null; // signal caller to advance to next block
            }
            return bcis.readLine();
        } else {
            return plainReader.readLine();
        }
    }

    private static boolean looksCompressed(String path) {
        String lower = path.toLowerCase();
        return lower.endsWith(".gz") || lower.endsWith(".bgz") || lower.endsWith(".bgzf");
    }

    /**
     * Detects BGZF format by reading 18 header bytes and checking for the BC subfield.
     * Leaves the stream position at byte 18 (caller must seek back to 0 if needed for
     * non-BGZF handling). For BGZF, BlockCompressedInputStream handles seeking internally.
     */
    private static boolean isBgzfStream(FSDataInputStream in) throws IOException {
        byte[] header = new byte[18];
        int read = in.read(header, 0, 18);
        if (read < 18) return false;
        // BGZF magic: gzip magic + FEXTRA flag + BC subfield
        return header[0] == (byte) 0x1F
            && header[1] == (byte) 0x8B
            && (header[3] & 0x04) != 0          // FEXTRA flag set
            && header[12] == (byte) 0x42         // SI1 = 'B'
            && header[13] == (byte) 0x43;        // SI2 = 'C'
    }

    // -------------------------------------------------------------------------
    // Field parsing helpers
    // -------------------------------------------------------------------------

    private static Integer parseIntOrNull(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty()) return null;
        try {
            // BED scores can be decimal strings like "818.0" — truncate
            int dot = s.indexOf('.');
            return Integer.parseInt(dot >= 0 ? s.substring(0, dot) : s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLongOrNull(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty()) return null;
        try {
            int dot = s.indexOf('.');
            return Long.parseLong(dot >= 0 ? s.substring(0, dot) : s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static UTF8String parseItemRgb(String rgb) {
        // Accept "R,G,B" or packed integer; normalize to "R,G,B"
        if (rgb.contains(",")) {
            return UTF8String.fromString(rgb);
        }
        // Packed integer
        try {
            int packed = Integer.parseInt(rgb);
            Color c = new Color(packed);
            return UTF8String.fromString(c.getRed() + "," + c.getGreen() + "," + c.getBlue());
        } catch (NumberFormatException e) {
            return UTF8String.fromString(rgb);
        }
    }

    /** Strip trailing comma from BED blockSizes/blockStarts fields (e.g. "100,200," → "100,200"). */
    private static String stripTrailingComma(String s) {
        return s.endsWith(",") ? s.substring(0, s.length() - 1) : s;
    }
}
