package com.litebfx.fastq;

import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.fastq.FastqReader;
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
 * <h3>Gzipped files</h3>
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

    private final FastqInputPartition partition;

    private FSDataInputStream rawStream;
    private FastqReader fastqReader;
    private FastqRecord current;
    private boolean exhausted = false;

    public FastqPartitionReader(FastqInputPartition partition) {
        log.trace("FastqPartitionReader(path={}, startByte={}, endByte={})",
                partition.getPath(), partition.getStartByte(), partition.getEndByte());
        this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
        if (fastqReader == null) {
            open();
        }
        if (exhausted) return false;

        if (!fastqReader.hasNext()) {
            exhausted = true;
            return false;
        }
        current = fastqReader.next();

        // For byte-range splits: stop when we have passed the end boundary.
        // We check after reading the record so we always emit complete records.
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
        values[4] = partition.getReadNumber(); // null when undetermined
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        log.trace("close() path={}", partition.getPath());
        if (fastqReader != null) {
            fastqReader.close();
        }
        if (rawStream != null) {
            rawStream.close();
        }
    }

    private void open() throws IOException {
        log.trace("open() path={}", partition.getPath());
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
     * Scans the stream byte-by-byte from its current position until it is
     * positioned at the {@code @} character of a FASTQ record header line.
     *
     * <p>Uses byte-by-byte reads (no buffering) so that
     * {@link FSDataInputStream#getPos()} remains accurate after each byte.
     */
    private static void advanceToRecordBoundary(FSDataInputStream stream) throws IOException {
        // Treat the start of a split as "just after a newline" so that if the
        // split happens to land exactly on a '@', we recognise it immediately.
        boolean prevWasNewline = true;
        long lastNewlineEnd = stream.getPos();
        int b;
        while ((b = stream.read()) != -1) {
            if (prevWasNewline && b == '@') {
                // Seek back to the '@' so FastqReader sees the full header line.
                stream.seek(lastNewlineEnd);
                return;
            }
            if (b == '\n') {
                prevWasNewline = true;
                lastNewlineEnd = stream.getPos(); // position of first byte on next line
            } else {
                prevWasNewline = false;
            }
        }
        // EOF reached without finding a record — leave stream at EOF.
    }
}
