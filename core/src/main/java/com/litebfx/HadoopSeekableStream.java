package com.litebfx;

import htsjdk.samtools.seekablestream.SeekableStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Adapts a Hadoop {@link FSDataInputStream} to the htsjdk {@link SeekableStream}
 * interface, allowing htsjdk to open BAM files on any Hadoop-compatible storage
 * (LocalFS, S3A, ADLS, DBFS, GCS, HDFS, …).
 */
public class HadoopSeekableStream extends SeekableStream {

    private static final Logger log = LoggerFactory.getLogger(HadoopSeekableStream.class);

    private final FSDataInputStream in;
    private final long length;
    private final String source;

    public HadoopSeekableStream(FSDataInputStream in, long length, String source) {
        log.trace("HadoopSeekableStream(source={}, length={})", source, length);
        this.in = in;
        this.length = length;
        this.source = source;
    }

    @Override
    public long length() {
        log.trace("length() -> {}", length);
        return length;
    }

    @Override
    public long position() throws IOException {
        long pos = in.getPos();
        if (log.isTraceEnabled()) log.trace("position() -> {}", pos);
        return pos;
    }

    @Override
    public void seek(long position) throws IOException {
        log.trace("seek(position={})", position);
        in.seek(position);
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (log.isTraceEnabled()) log.trace("read() -> {}", b);
        return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        int n = in.read(buffer, offset, length);
        if (log.isTraceEnabled()) log.trace("read(offset={}, length={}) -> {} bytes", offset, length, n);
        return n;
    }

    @Override
    public void close() throws IOException {
        log.trace("close() source={}", source);
        in.close();
    }

    @Override
    public boolean eof() throws IOException {
        boolean atEof = in.getPos() >= length;
        if (log.isTraceEnabled()) log.trace("eof() -> {}", atEof);
        return atEof;
    }

    @Override
    public String getSource() {
        log.trace("getSource() -> {}", source);
        return source;
    }
}
