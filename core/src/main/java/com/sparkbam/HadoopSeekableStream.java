package com.litebfx;

import htsjdk.samtools.seekablestream.SeekableStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * Adapts a Hadoop {@link FSDataInputStream} to the htsjdk {@link SeekableStream}
 * interface, allowing htsjdk to open BAM files on any Hadoop-compatible storage
 * (LocalFS, S3A, ADLS, DBFS, GCS, HDFS, …).
 */
public class HadoopSeekableStream extends SeekableStream {

    private final FSDataInputStream in;
    private final long length;
    private final String source;

    public HadoopSeekableStream(FSDataInputStream in, long length, String source) {
        this.in = in;
        this.length = length;
        this.source = source;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long position() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long position) throws IOException {
        in.seek(position);
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        return in.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public boolean eof() throws IOException {
        return in.getPos() >= length;
    }

    @Override
    public String getSource() {
        return source;
    }
}
