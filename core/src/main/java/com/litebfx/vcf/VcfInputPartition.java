package com.litebfx.vcf;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a VCF/BCF file.
 *
 * <p>When {@code queryChrom} is non-null, the reader performs a tabix region
 * query; otherwise it performs a full-file scan.
 */
public class VcfInputPartition implements InputPartition, Serializable {

    private final String path;
    private final String indexPath;
    /** Null means full-file scan; non-null means tabix region query. */
    private final String queryChrom;
    private final int queryStart;  // 1-based
    private final int queryEnd;    // 1-based inclusive
    private final SerializableConfiguration hadoopConf;

    /** Full-file partition — no region filter, no index. */
    public VcfInputPartition(String path,
                             String indexPath,
                             Configuration hadoopConf) {
        this(path, indexPath, null, 1, Integer.MAX_VALUE, hadoopConf);
    }

    public VcfInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             int queryStart,
                             int queryEnd,
                             Configuration hadoopConf) {
        this.path       = path;
        this.indexPath  = indexPath;
        this.queryChrom = queryChrom;
        this.queryStart = queryStart;
        this.queryEnd   = queryEnd;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    public String getPath()        { return path; }
    public String getIndexPath()   { return indexPath; }
    public String getQueryChrom()  { return queryChrom; }
    public int    getQueryStart()  { return queryStart; }
    public int    getQueryEnd()    { return queryEnd; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
}
