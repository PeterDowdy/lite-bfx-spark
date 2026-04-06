package com.litebfx.bed;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a BED/BED.GZ file.
 *
 * <p>When {@code queryChrom} is non-null, the reader performs a tabix region
 * query; otherwise it performs a full-file scan.
 */
public class BedInputPartition implements InputPartition, Serializable {

    private final String path;
    private final String indexPath;
    /** Null means full-file scan; non-null means tabix region query. */
    private final String queryChrom;
    private final long queryStart;
    private final long queryEnd;
    private final SerializableConfiguration hadoopConf;

    /** Full-file partition — no region filter, no tabix. */
    public BedInputPartition(String path,
                             String indexPath,
                             Configuration hadoopConf) {
        this(path, indexPath, null, 0L, Long.MAX_VALUE, hadoopConf);
    }

    public BedInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             long queryStart,
                             long queryEnd,
                             Configuration hadoopConf) {
        this.path       = path;
        this.indexPath  = indexPath;
        this.queryChrom = queryChrom;
        this.queryStart = queryStart;
        this.queryEnd   = queryEnd;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    public String getPath() { return path; }
    public String getIndexPath() { return indexPath; }
    public String getQueryChrom() { return queryChrom; }
    public long getQueryStart() { return queryStart; }
    public long getQueryEnd() { return queryEnd; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
}
