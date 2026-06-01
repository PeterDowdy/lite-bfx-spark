package com.litebfx.bed;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Describes a single Spark partition over a BED/BED.GZ file.
 *
 * <p>When {@code queryChrom} is non-null, the reader performs a tabix region
 * query; otherwise it performs a full-file scan.
 *
 * <p>For plain-text (uncompressed) BED files, {@code startByte} and {@code endByte}
 * delimit a byte-range chunk of the file.  The reader seeks to {@code startByte},
 * discards bytes up to the next newline to land on a clean line start, and reads
 * records until the next line would begin at or past {@code endByte}.
 */
public class BedInputPartition implements InputPartition {

    private final String path;
    private final String indexPath;
    /** Null means full-file scan; non-null means tabix region query. */
    private final String queryChrom;
    private final long queryStart;
    private final long queryEnd;
    /** Byte offset where this partition's chunk begins (0 for full-file or tabix). */
    private final long startByte;
    /** Exclusive byte offset where this partition's chunk ends (Long.MAX_VALUE = EOF). */
    private final long endByte;
    private final SerializableConfiguration hadoopConf;
    /** Maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    private final int rowLimit;

    /** Full-file partition — no region filter, no tabix, no byte range. */
    public BedInputPartition(String path,
                             String indexPath,
                             Configuration hadoopConf) {
        this(path, indexPath, null, 0L, Long.MAX_VALUE, 0L, Long.MAX_VALUE, hadoopConf);
    }

    /** Tabix region query partition. */
    public BedInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             long queryStart,
                             long queryEnd,
                             Configuration hadoopConf) {
        this(path, indexPath, queryChrom, queryStart, queryEnd, 0L, Long.MAX_VALUE, hadoopConf);
    }

    /** Primary constructor — used for line-split plain-text partitions. */
    public BedInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             long queryStart,
                             long queryEnd,
                             long startByte,
                             long endByte,
                             Configuration hadoopConf) {
        this(path, indexPath, queryChrom, queryStart, queryEnd, startByte, endByte,
             hadoopConf, Integer.MAX_VALUE);
    }

    private BedInputPartition(String path,
                              String indexPath,
                              String queryChrom,
                              long queryStart,
                              long queryEnd,
                              long startByte,
                              long endByte,
                              Configuration hadoopConf,
                              int rowLimit) {
        this.path       = path;
        this.indexPath  = indexPath;
        this.queryChrom = queryChrom;
        this.queryStart = queryStart;
        this.queryEnd   = queryEnd;
        this.startByte  = startByte;
        this.endByte    = endByte;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
        this.rowLimit   = rowLimit;
    }

    /** Returns a copy of this partition with the given row limit. */
    public BedInputPartition withRowLimit(int limit) {
        return new BedInputPartition(path, indexPath, queryChrom, queryStart, queryEnd,
                startByte, endByte, hadoopConf.get(), limit);
    }

    public String getPath() { return path; }
    public String getIndexPath() { return indexPath; }
    public String getQueryChrom() { return queryChrom; }
    public long getQueryStart() { return queryStart; }
    public long getQueryEnd() { return queryEnd; }
    public long getStartByte() { return startByte; }
    public long getEndByte() { return endByte; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    /** Returns the maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    public int getRowLimit() { return rowLimit; }
}
