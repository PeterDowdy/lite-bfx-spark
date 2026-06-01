package com.litebfx.bam;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;


/**
 * Describes a single Spark partition over a BAM/SAM/CRAM file.
 *
 * <p>Create instances via the static factory methods — one per partition mode:
 * <ul>
 *   <li>{@link #forFullScan} — full file or single BAI/CRAI-guided region</li>
 *   <li>{@link #forRegionQuery} — single pushed-down region with BAI</li>
 *   <li>{@link #forVfoPartitions} — per-reference VFO partitioning (BAM + BAI)</li>
 *   <li>{@link #forUnmapped} — unplaced unmapped reads (BAM + BAI)</li>
 *   <li>{@link #forBgzfSplit} — unindexed BAM byte-range split</li>
 *   <li>{@link #forSamSplit} — SAM text line-range split</li>
 *   <li>{@link #forCramContainerSplit} — CRAM container byte-span split</li>
 * </ul>
 *
 * <p>The Hadoop {@link Configuration} is carried per-partition so that executors
 * can reconstruct a {@link org.apache.hadoop.fs.FileSystem} with the correct
 * S3A/ADLS credentials even when they differ from the default Hadoop config.
 */
public class BamInputPartition implements InputPartition {

    // ---- fields shared by all modes ----
    private final String path;
    private final SerializableConfiguration hadoopConf;
    /** Resolved BAI/CRAI path; null when no index is available. */
    private final String indexPath;
    private final boolean isCram;
    /** Path to FASTA reference file; null when not provided. CRAM only. */
    private final String referenceFile;
    /** Reference resolution mode: "file", "md5", or "none". CRAM only. */
    private final String referenceMode;

    // ---- byte-range fields (BGZF split, SAM split) ----
    private final long startByte;
    private final long endByte;

    // ---- query fields (region push-down, VFO, unmapped) ----
    private final String querySequence;
    private final int queryStart;
    private final int queryEnd;
    private final String[] querySequences;
    private final boolean queryUnmapped;

    // ---- split-mode flags ----
    private final boolean samSplit;

    // ---- CRAM container-split ----
    private final long[] cramContainerSpans;

    // ---- limit pushdown ----
    private final int rowLimit;

    // -------------------------------------------------------------------------
    // Factory methods — one per partition mode
    // -------------------------------------------------------------------------

    /**
     * Full-file scan, or a single BAI/CRAI-guided region query (when {@code querySequence}
     * is non-null). This is the simplest partition: no byte-range splitting, no VFO grouping.
     */
    public static BamInputPartition forFullScan(String path,
                                                Configuration conf,
                                                String indexPath,
                                                boolean isCram,
                                                String referenceFile,
                                                String referenceMode) {
        return new BamInputPartition(path, conf, indexPath, isCram, referenceFile, referenceMode,
                0L, Long.MAX_VALUE,
                null, 1, Integer.MAX_VALUE,
                null, false, false, null);
    }

    /** Convenience overload for plain BAM full-scan with no index and no CRAM settings. */
    public static BamInputPartition forFullScan(String path, Configuration conf) {
        return forFullScan(path, conf, null, false, null, "none");
    }

    /**
     * Single pushed-down region query using a BAI/CRAI index.
     * {@code querySequence} must be non-null; the reader calls
     * {@code samReader.query(querySequence, queryStart, queryEnd, false)}.
     */
    public static BamInputPartition forRegionQuery(String path,
                                                   Configuration conf,
                                                   String indexPath,
                                                   boolean isCram,
                                                   String referenceFile,
                                                   String referenceMode,
                                                   String querySequence,
                                                   int queryStart,
                                                   int queryEnd) {
        return new BamInputPartition(path, conf, indexPath, isCram, referenceFile, referenceMode,
                0L, Long.MAX_VALUE,
                querySequence, queryStart, queryEnd,
                null, false, false, null);
    }

    /**
     * Per-reference VFO partitioning (BAM + BAI).
     * The reader builds a {@link htsjdk.samtools.QueryInterval}{@code []} from {@code querySequences}
     * and calls {@code samReader.query(intervals, false)}; htsjdk uses the BAI VFOs internally.
     */
    public static BamInputPartition forVfoPartitions(String path,
                                                     Configuration conf,
                                                     String indexPath,
                                                     String referenceFile,
                                                     String referenceMode,
                                                     String[] querySequences) {
        return new BamInputPartition(path, conf, indexPath, false, referenceFile, referenceMode,
                0L, Long.MAX_VALUE,
                null, 1, Integer.MAX_VALUE,
                querySequences, false, false, null);
    }

    /**
     * Unplaced unmapped reads partition (BAM + BAI).
     * The reader calls {@code samReader.queryUnmapped()}.
     */
    public static BamInputPartition forUnmapped(String path,
                                                Configuration conf,
                                                String indexPath,
                                                String referenceFile,
                                                String referenceMode) {
        return new BamInputPartition(path, conf, indexPath, false, referenceFile, referenceMode,
                0L, Long.MAX_VALUE,
                null, 1, Integer.MAX_VALUE,
                null, true, false, null);
    }

    /**
     * Byte-range BGZF split for an unindexed BAM.
     * The reader scans from {@code startByte} to the first clean BGZF block boundary and reads
     * records using {@link htsjdk.samtools.BAMRecordCodec} until {@code endByte} is reached.
     */
    public static BamInputPartition forBgzfSplit(String path,
                                                 Configuration conf,
                                                 long startByte,
                                                 long endByte,
                                                 String referenceFile,
                                                 String referenceMode) {
        return new BamInputPartition(path, conf, null, false, referenceFile, referenceMode,
                startByte, endByte,
                null, 1, Integer.MAX_VALUE,
                null, false, false, null);
    }

    /**
     * SAM text line-range split.
     * The reader seeks to {@code startByte}, discards bytes up to the next newline, and reads
     * SAM text lines until the line-start position reaches {@code endByte}.
     */
    public static BamInputPartition forSamSplit(String path,
                                                Configuration conf,
                                                long startByte,
                                                long endByte) {
        return new BamInputPartition(path, conf, null, false, null, "none",
                startByte, endByte,
                null, 1, Integer.MAX_VALUE,
                null, false, true, null);
    }

    /**
     * CRAM container byte-span split.
     * {@code cramContainerSpans} carries alternating {@code [start₀, end₀, start₁, end₁, …]} VFO
     * pairs; the reader creates a {@code CRAMIterator} over these spans and yields all records
     * in those containers. An empty array produces an empty partition.
     */
    public static BamInputPartition forCramContainerSplit(String path,
                                                          Configuration conf,
                                                          String indexPath,
                                                          String referenceFile,
                                                          String referenceMode,
                                                          long[] cramContainerSpans) {
        return new BamInputPartition(path, conf, indexPath, true, referenceFile, referenceMode,
                0L, Long.MAX_VALUE,
                null, 1, Integer.MAX_VALUE,
                null, false, false, cramContainerSpans);
    }

    // -------------------------------------------------------------------------
    // Single private constructor — all fields, all modes
    // -------------------------------------------------------------------------

    private BamInputPartition(String path,
                              Configuration conf,
                              String indexPath,
                              boolean isCram,
                              String referenceFile,
                              String referenceMode,
                              long startByte,
                              long endByte,
                              String querySequence,
                              int queryStart,
                              int queryEnd,
                              String[] querySequences,
                              boolean queryUnmapped,
                              boolean samSplit,
                              long[] cramContainerSpans) {
        this(path, conf, indexPath, isCram, referenceFile, referenceMode,
             startByte, endByte, querySequence, queryStart, queryEnd,
             querySequences, queryUnmapped, samSplit, cramContainerSpans, Integer.MAX_VALUE);
    }

    private BamInputPartition(String path,
                              Configuration conf,
                              String indexPath,
                              boolean isCram,
                              String referenceFile,
                              String referenceMode,
                              long startByte,
                              long endByte,
                              String querySequence,
                              int queryStart,
                              int queryEnd,
                              String[] querySequences,
                              boolean queryUnmapped,
                              boolean samSplit,
                              long[] cramContainerSpans,
                              int rowLimit) {
        this.path              = path;
        this.hadoopConf        = new SerializableConfiguration(conf);
        this.indexPath         = indexPath;
        this.isCram            = isCram;
        this.referenceFile     = referenceFile;
        this.referenceMode     = referenceMode;
        this.startByte         = startByte;
        this.endByte           = endByte;
        this.querySequence     = querySequence;
        this.queryStart        = queryStart;
        this.queryEnd          = queryEnd;
        this.querySequences    = querySequences;
        this.queryUnmapped     = queryUnmapped;
        this.samSplit          = samSplit;
        this.cramContainerSpans = cramContainerSpans;
        this.rowLimit          = rowLimit;
    }

    /** Returns a copy of this partition with the given row limit. */
    public BamInputPartition withRowLimit(int limit) {
        return new BamInputPartition(path, hadoopConf.get(), indexPath, isCram,
                referenceFile, referenceMode, startByte, endByte,
                querySequence, queryStart, queryEnd, querySequences,
                queryUnmapped, samSplit, cramContainerSpans, limit);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String getPath()            { return path; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    public String getIndexPath()       { return indexPath; }
    public boolean isCram()            { return isCram; }
    public String getReferenceFile()   { return referenceFile; }
    public String getReferenceMode()   { return referenceMode; }
    public long getStartByte()         { return startByte; }
    public long getEndByte()           { return endByte; }
    public String getQuerySequence()   { return querySequence; }
    public int getQueryStart()         { return queryStart; }
    public int getQueryEnd()           { return queryEnd; }
    /** Returns the reference names for per-reference VFO partitioning, or null for other modes. */
    public String[] getQuerySequences()  { return querySequences; }
    /** Returns true if this partition should read only unplaced unmapped reads. */
    public boolean isQueryUnmapped()     { return queryUnmapped; }
    /** Returns true if this partition uses SAM line-based splitting. */
    public boolean isSamSplit()          { return samSplit; }
    /**
     * Returns the alternating {@code [start, end]} container byte-offset spans for CRAM
     * container-split mode, or {@code null} for all other partition modes.
     */
    public long[] getCramContainerSpans() { return cramContainerSpans; }
    /** Returns the maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    public int rowLimit() { return rowLimit; }
}
