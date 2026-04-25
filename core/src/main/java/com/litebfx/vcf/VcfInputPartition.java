package com.litebfx.vcf;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a VCF/BCF file.
 *
 * <p>When {@code queryChrom} is non-null, the reader performs a tabix region
 * query for that single chromosome; otherwise it performs a full-file scan.
 *
 * <p>When {@code queryChroms} is non-null, the reader iterates tabix region
 * queries for each chromosome in the array in order (used for grouped
 * per-chromosome partitions).  {@code queryChroms} takes precedence over
 * {@code queryChrom} when both are set.
 *
 * <p>For plain-text (uncompressed) VCF files, {@code startByte} and {@code endByte}
 * delimit a byte-range chunk of the file.  The reader seeks to {@code startByte},
 * discards bytes up to the next newline to land on a clean line start, and reads
 * records until the next line would begin at or past {@code endByte}.
 */
public class VcfInputPartition implements InputPartition, Serializable {

    private final String path;
    private final String indexPath;
    /** Null means full-file scan; non-null means tabix region query for one chrom. */
    private final String queryChrom;
    private final int queryStart;  // 1-based
    private final int queryEnd;    // 1-based inclusive
    /**
     * Non-null: reader iterates tabix queries for each chrom in this array (grouped
     * per-chromosome partitions).  Takes precedence over {@code queryChrom}.
     */
    private final String[] queryChroms;
    /** Byte offset where this partition's chunk begins (0 for full-file or tabix). */
    private final long startByte;
    /** Exclusive byte offset where this partition's chunk ends (Long.MAX_VALUE = EOF). */
    private final long endByte;
    private final SerializableConfiguration hadoopConf;

    /** Full-file partition — no region filter, no index. */
    public VcfInputPartition(String path,
                             String indexPath,
                             Configuration hadoopConf) {
        this(path, indexPath, null, 1, Integer.MAX_VALUE, null, 0L, Long.MAX_VALUE, hadoopConf);
    }

    /** Tabix region query partition (single chromosome). */
    public VcfInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             int queryStart,
                             int queryEnd,
                             Configuration hadoopConf) {
        this(path, indexPath, queryChrom, queryStart, queryEnd, null, 0L, Long.MAX_VALUE, hadoopConf);
    }

    /** Multi-chrom tabix partition — reader chains one tabix query per chrom in {@code queryChroms}. */
    public VcfInputPartition(String path,
                             String indexPath,
                             String[] queryChroms,
                             Configuration hadoopConf) {
        this(path, indexPath, null, 1, Integer.MAX_VALUE, queryChroms, 0L, Long.MAX_VALUE, hadoopConf);
    }

    /** Line-split plain-text partition. */
    public VcfInputPartition(String path,
                             String indexPath,
                             String queryChrom,
                             int queryStart,
                             int queryEnd,
                             long startByte,
                             long endByte,
                             Configuration hadoopConf) {
        this(path, indexPath, queryChrom, queryStart, queryEnd, null, startByte, endByte, hadoopConf);
    }

    /** Primary constructor. */
    private VcfInputPartition(String path,
                              String indexPath,
                              String queryChrom,
                              int queryStart,
                              int queryEnd,
                              String[] queryChroms,
                              long startByte,
                              long endByte,
                              Configuration hadoopConf) {
        this.path        = path;
        this.indexPath   = indexPath;
        this.queryChrom  = queryChrom;
        this.queryStart  = queryStart;
        this.queryEnd    = queryEnd;
        this.queryChroms = queryChroms;
        this.startByte   = startByte;
        this.endByte     = endByte;
        this.hadoopConf  = new SerializableConfiguration(hadoopConf);
    }

    public String   getPath()        { return path; }
    public String   getIndexPath()   { return indexPath; }
    public String   getQueryChrom()  { return queryChrom; }
    public int      getQueryStart()  { return queryStart; }
    public int      getQueryEnd()    { return queryEnd; }
    public String[] getQueryChroms() { return queryChroms; }
    public long     getStartByte()   { return startByte; }
    public long     getEndByte()     { return endByte; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
}
