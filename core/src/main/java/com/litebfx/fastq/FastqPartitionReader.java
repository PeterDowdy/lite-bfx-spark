package com.litebfx.fastq;

import com.litebfx.HadoopSeekableStream;
import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Reads {@link FastqRecord}s from a single {@link FastqInputPartition} and converts them
 * to Spark {@link InternalRow}s matching {@link FastqSchema#SCHEMA}.
 *
 * <h3>BGZF byte-range splits</h3>
 * When {@link FastqInputPartition#isBgzf()} is true the file is BGZF-compressed and this
 * partition covers a compressed-byte range [{@code startByte}, {@code endByte}).
 * The reader seeks to the first BGZF block at or after {@code startByte} (using the 4-byte
 * BGZF magic to locate the boundary), then scans forward in the decompressed byte stream to
 * the next line starting with {@code @} before beginning iteration. Records are read until
 * the compressed block address returned by {@link BlockCompressedInputStream#getFilePointer()}
 * meets or exceeds {@code endByte}.
 *
 * <h3>Plain-gzip files</h3>
 * The partition spans the entire file ({@code startByte=0, endByte=Long.MAX_VALUE}).
 * A {@link GZIPInputStream} is layered over the {@link FSDataInputStream}.
 *
 * <h3>Uncompressed byte-range splits</h3>
 * The reader seeks to {@code startByte}, then scans forward byte-by-byte to the
 * next line starting with {@code @} before beginning iteration. Records are emitted
 * until the stream position exceeds {@code endByte}.
 */
public class FastqPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(FastqPartitionReader.class);

    /** BGZF block magic bytes: gzip ID1/ID2, CM=deflate, FLG=extra-field. */
    private static final byte[] BGZF_MAGIC = {(byte) 0x1f, (byte) 0x8b, 0x08, 0x04};

    private final FastqInputPartition partition;

    // ── plain-gzip and uncompressed mode ───────────────────────────────────────
    private FSDataInputStream rawStream;
    private FastqReader fastqReader;

    // ── BGZF split mode ────────────────────────────────────────────────────────
    private FSDataInputStream bgzfFsStream;
    private BlockCompressedInputStream bcis;

    // ── shared ─────────────────────────────────────────────────────────────────
    private boolean opened   = false;
    private boolean exhausted = false;
    private FastqRecord current;

    public FastqPartitionReader(FastqInputPartition partition) {
        log.trace("FastqPartitionReader(path={}, startByte={}, endByte={}, bgzf={})",
                partition.getPath(), partition.getStartByte(), partition.getEndByte(), partition.isBgzf());
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
        if (exhausted) return false;

        if (bcis != null) {
            return nextBgzf();
        }

        // plain-gzip / uncompressed path via FastqReader
        if (!fastqReader.hasNext()) {
            exhausted = true;
            return false;
        }
        current = fastqReader.next();

        if (partition.getEndByte() != Long.MAX_VALUE) {
            try {
                long pos = rawStream.getPos();
                if (pos > partition.getEndByte()) {
                    exhausted = true;
                }
            } catch (IOException e) {
                log.trace("next() could not check stream position: {}", e.getMessage());
            }
        }

        log.trace("next() readName={}", current.getReadName());
        return true;
    }

    @Override
    public InternalRow get() {
        String header = current.getReadName();

        // htsjdk strips the leading '@'. Split on first space to separate
        // the read name from the optional description.
        String readName;
        String description;
        int spaceIdx = header.indexOf(' ');
        if (spaceIdx >= 0) {
            readName = header.substring(0, spaceIdx);
            String desc = header.substring(spaceIdx + 1);
            description = desc.isEmpty() ? null : desc;
        } else {
            readName = header;
            description = null;
        }

        Object[] values = new Object[5];
        values[0] = UTF8String.fromString(readName);
        values[1] = UTF8String.fromString(current.getReadString());
        values[2] = UTF8String.fromString(current.getBaseQualityString());
        values[3] = description != null ? UTF8String.fromString(description) : null;
        values[4] = partition.getReadNumber();
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        log.trace("close() path={}", partition.getPath());
        try {
            if (bcis != null) {
                bcis.close(); // closes HadoopSeekableStream → bgzfFsStream
                bgzfFsStream = null;
            }
            if (fastqReader != null) {
                fastqReader.close();
            }
        } finally {
            if (bgzfFsStream != null) bgzfFsStream.close();
            if (rawStream != null) rawStream.close();
        }
    }

    // -------------------------------------------------------------------------
    // Open helpers
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        log.trace("open() path={}", partition.getPath());

        if (partition.isBgzf()) {
            openBgzfSplit();
            return;
        }

        Path hadoopPath = new Path(partition.getPath());
        FileSystem fs = hadoopPath.getFileSystem(partition.getHadoopConf());
        rawStream = fs.open(hadoopPath);

        InputStream inputStream;
        String pathLower = partition.getPath().toLowerCase();
        boolean isGzipped = pathLower.endsWith(".fastq.gz") || pathLower.endsWith(".fq.gz");

        if (isGzipped) {
            inputStream = new GZIPInputStream(rawStream);
        } else {
            long startByte = partition.getStartByte();
            if (startByte > 0) {
                rawStream.seek(startByte);
                advanceToRecordBoundary(rawStream);
            }
            inputStream = rawStream;
        }

        fastqReader = new FastqReader(
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)));
        log.trace("open() FastqReader opened, isGzipped={}", isGzipped);
    }

    /**
     * Opens the partition in BGZF split mode.
     *
     * <p>For partitions with {@code startByte > 0}: scans raw bytes starting at
     * {@code startByte} for the 4-byte BGZF magic to find the first block boundary,
     * then seeks the {@link BlockCompressedInputStream} to that block's virtual file offset.
     * For partition 0 ({@code startByte == 0}): begins at the start of the file.
     *
     * <p>After positioning, scans the decompressed byte stream for the first line
     * beginning with {@code @}, seeking back to that position so the first record
     * header is included.  If no BGZF block is found in the partition's range the
     * partition is marked empty.
     */
    private void openBgzfSplit() throws IOException {
        log.trace("openBgzfSplit() startByte={} endByte={}",
                partition.getStartByte(), partition.getEndByte());
        Path hadoopPath = new Path(partition.getPath());
        FileSystem fs = hadoopPath.getFileSystem(partition.getHadoopConf());
        long fileLength = fs.getFileStatus(hadoopPath).getLen();

        bgzfFsStream = fs.open(hadoopPath);
        boolean success = false;
        try {
            bcis = new BlockCompressedInputStream(
                    new HadoopSeekableStream(bgzfFsStream, fileLength, partition.getPath()));

            if (partition.getStartByte() > 0) {
                long blockStart = findNextBgzfBlockStart(bgzfFsStream, partition.getStartByte());
                if (blockStart < 0
                        || (partition.getEndByte() != Long.MAX_VALUE && blockStart >= partition.getEndByte())) {
                    log.trace("openBgzfSplit() no BGZF block found in range — empty partition");
                    exhausted = true;
                    success = true;
                    return;
                }
                log.trace("openBgzfSplit() first block at compressed offset={}", blockStart);
                bcis.seek(BlockCompressedFilePointerUtil.makeFilePointer(blockStart));
            }
            // else partition 0: BCIS starts at VFO 0 (beginning of file)

            advanceToRecordBoundaryBgzf(bcis);
            log.trace("openBgzfSplit() positioned at first '@'");
            success = true;
        } finally {
            if (!success) {
                if (bcis != null) {
                    try { bcis.close(); } catch (IOException e) {
                        log.debug("suppressed exception closing BCIS", e);
                    }
                    bgzfFsStream = null; // bcis.close() closed it via HadoopSeekableStream
                } else {
                    bgzfFsStream.close();
                    bgzfFsStream = null;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // BGZF record iteration
    // -------------------------------------------------------------------------

    /**
     * Reads the next FASTQ record directly from the {@link BlockCompressedInputStream},
     * one byte at a time, giving accurate per-record position tracking.
     *
     * <p>Before reading each record the compressed block address is compared to
     * {@code endByte}; if it meets or exceeds that boundary the partition is exhausted.
     */
    private boolean nextBgzf() throws IOException {
        if (partition.getEndByte() != Long.MAX_VALUE) {
            long blockAddr = BlockCompressedFilePointerUtil.getBlockAddress(bcis.getFilePointer());
            if (blockAddr >= partition.getEndByte()) {
                log.trace("nextBgzf() reached end boundary blockAddr={} endByte={}",
                        blockAddr, partition.getEndByte());
                exhausted = true;
                return false;
            }
        }

        // Read the '@header' line
        String header = readLineBcis();
        if (header == null) { exhausted = true; return false; }
        // Tolerate blank lines between records (non-standard but seen in the wild)
        while (header.isEmpty() || header.charAt(0) != '@') {
            header = readLineBcis();
            if (header == null) { exhausted = true; return false; }
        }

        String sequence = readLineBcis();
        if (sequence == null) { exhausted = true; return false; }

        readLineBcis(); // consume '+' separator — content is not used

        String quals = readLineBcis();
        if (quals == null) { exhausted = true; return false; }

        // Strip the leading '@' so FastqRecord.getReadName() returns a bare name,
        // matching the behaviour of FastqReader (which strips '@' before constructing
        // the record).  get() then splits the result on the first space to separate
        // readName from the optional description.
        String readHeader = (header.length() > 0 && header.charAt(0) == '@')
                ? header.substring(1) : header;
        current = new FastqRecord(readHeader, sequence, "+", quals);
        log.trace("nextBgzf() readName={}", current.getReadName());
        return true;
    }

    /**
     * Reads one line from the {@link BlockCompressedInputStream} byte-by-byte.
     * Returns the line content without the trailing newline, or {@code null} at EOF
     * when no bytes were read.
     *
     * <p>Byte-by-byte reads are efficient because BCIS buffers a full decompressed
     * block (~128 KB) internally; each call simply increments an index in that buffer.
     * This approach also keeps {@link BlockCompressedInputStream#getFilePointer()} accurate
     * to within one byte, which is critical for the end-boundary check in {@link #nextBgzf()}.
     */
    private String readLineBcis() throws IOException {
        StringBuilder sb = new StringBuilder(256);
        int b;
        while ((b = bcis.read()) != -1) {
            if (b == '\n') return sb.toString();
            if (b != '\r') sb.append((char) b);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    // -------------------------------------------------------------------------
    // Static helpers
    // -------------------------------------------------------------------------

    /**
     * Scans the stream byte-by-byte from its current position until it is
     * positioned at the {@code @} character of a FASTQ record header line.
     *
     * <p>Uses byte-by-byte reads (no buffering) so that
     * {@link FSDataInputStream#getPos()} remains accurate after each byte.
     */
    private static void advanceToRecordBoundary(FSDataInputStream stream) throws IOException {
        boolean prevWasNewline = true;
        long lastNewlineEnd = stream.getPos();
        int b;
        while ((b = stream.read()) != -1) {
            if (prevWasNewline && b == '@') {
                stream.seek(lastNewlineEnd);
                return;
            }
            if (b == '\n') {
                prevWasNewline = true;
                lastNewlineEnd = stream.getPos();
            } else {
                prevWasNewline = false;
            }
        }
    }

    /**
     * Scans the decompressed BCIS stream byte-by-byte until it is positioned at
     * the {@code @} character of a FASTQ record header line, then seeks back via
     * {@link BlockCompressedInputStream#seek(long)} so the full header line is visible
     * to the subsequent {@link #readLineBcis()} call.
     */
    private static void advanceToRecordBoundaryBgzf(BlockCompressedInputStream bcis)
            throws IOException {
        boolean prevWasNewline = true;
        long lastNewlineVfo = bcis.getFilePointer();
        int b;
        while ((b = bcis.read()) != -1) {
            if (prevWasNewline && b == '@') {
                bcis.seek(lastNewlineVfo);
                return;
            }
            if (b == '\n') {
                prevWasNewline = true;
                lastNewlineVfo = bcis.getFilePointer();
            } else {
                prevWasNewline = false;
            }
        }
        // EOF reached without finding '@' — leave BCIS at EOF.
    }

    /**
     * Scans raw bytes starting at {@code startByte} for the first occurrence of the
     * BGZF 4-byte magic ({@code 0x1f 0x8b 0x08 0x04}).
     *
     * <p>Reads one buffer of {@code MAX_COMPRESSED_BLOCK_SIZE + 4} bytes — large enough
     * to always contain the start of the next contiguous BGZF block regardless of where
     * {@code startByte} falls within the preceding block.
     *
     * @return compressed file offset of the first magic match, or -1 if not found
     */
    private static long findNextBgzfBlockStart(FSDataInputStream stream, long startByte)
            throws IOException {
        stream.seek(startByte);
        int bufSize = BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE + 4;
        byte[] buf = new byte[bufSize];
        int total = 0;
        while (total < bufSize) {
            int n = stream.read(buf, total, bufSize - total);
            if (n < 0) break;
            total += n;
        }
        for (int i = 0; i <= total - 4; i++) {
            if (buf[i] == BGZF_MAGIC[0] && buf[i + 1] == BGZF_MAGIC[1]
                    && buf[i + 2] == BGZF_MAGIC[2] && buf[i + 3] == BGZF_MAGIC[3]) {
                return startByte + i;
            }
        }
        return -1L;
    }
}
