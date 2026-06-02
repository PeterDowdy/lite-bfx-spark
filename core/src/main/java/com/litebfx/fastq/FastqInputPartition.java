package com.litebfx.fastq;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.HasPartitionStatistics;

import java.util.OptionalLong;

/**
 * Describes a single Spark partition over a FASTQ file.
 *
 * <p>For plain-gzip files ({@code .fastq.gz} / {@code .fq.gz}) that are not BGZF,
 * a single partition is created with {@code startByte=0} and {@code endByte=Long.MAX_VALUE}
 * because a plain gzip stream cannot be seeked into.
 *
 * <p>For BGZF-compressed files, multiple partitions are created with byte-range boundaries
 * in the compressed address space. Each reader seeks to the first BGZF block at or after
 * {@code startByte}, then advances to the next {@code @} record boundary before reading.
 *
 * <p>For uncompressed files, the file is divided into byte-range splits.
 * Each reader seeks to {@code startByte} then advances forward to the next
 * {@code @} record boundary before beginning iteration.
 */
public class FastqInputPartition implements HasPartitionStatistics {

    private final String path;
    private final long startByte;
    private final long endByte;
    private final SerializableConfiguration hadoopConf;
    /** 1 for R1, 2 for R2, null when undetermined. */
    private final Integer readNumber;
    /**
     * True when this partition covers a byte range of a BGZF-compressed file.
     * False for plain-gzip (single full-file partition) and uncompressed byte-range splits.
     */
    private final boolean bgzf;
    /** Maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    private final int rowLimit;

    public FastqInputPartition(String path, long startByte, long endByte, Configuration hadoopConf) {
        this(path, startByte, endByte, hadoopConf, null, false, Integer.MAX_VALUE);
    }

    public FastqInputPartition(String path, long startByte, long endByte,
                               Configuration hadoopConf, Integer readNumber) {
        this(path, startByte, endByte, hadoopConf, readNumber, false, Integer.MAX_VALUE);
    }

    public FastqInputPartition(String path, long startByte, long endByte,
                               Configuration hadoopConf, Integer readNumber, boolean bgzf) {
        this(path, startByte, endByte, hadoopConf, readNumber, bgzf, Integer.MAX_VALUE);
    }

    public FastqInputPartition(String path, long startByte, long endByte,
                               Configuration hadoopConf, Integer readNumber, boolean bgzf,
                               int rowLimit) {
        this.path = path;
        this.startByte = startByte;
        this.endByte = endByte;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
        this.readNumber = readNumber;
        this.bgzf = bgzf;
        this.rowLimit = rowLimit;
    }

    public String getPath() { return path; }
    public long getStartByte() { return startByte; }
    public long getEndByte() { return endByte; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    /** Returns 1 for R1, 2 for R2, or null when the read number is undetermined. */
    public Integer getReadNumber() { return readNumber; }
    /** Returns true when this partition is a byte-range split of a BGZF-compressed file. */
    public boolean isBgzf() { return bgzf; }
    /** Returns the maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    public int getRowLimit() { return rowLimit; }

    @Override
    public OptionalLong sizeInBytes() {
        if (endByte != Long.MAX_VALUE) return OptionalLong.of(endByte - startByte);
        return OptionalLong.empty();
    }

    @Override public OptionalLong numRows()    { return OptionalLong.empty(); }
    @Override public OptionalLong filesCount() { return OptionalLong.of(1L); }
}
