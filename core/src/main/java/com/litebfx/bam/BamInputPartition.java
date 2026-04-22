package com.litebfx.bam;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a BAM/SAM/CRAM file.
 *
 * <p>Virtual file offsets (VFOs) are used indirectly: when a BAI index is available,
 * {@link BamScan} creates one partition per reference sequence (or group of sequences).
 * Each partition's reader calls {@code samReader.query()} which has htsjdk use the BAI's
 * VFO chunks internally for efficient seeking. {@code startByte} and {@code endByte} are
 * raw file byte offsets used in BGZF split mode; they are 0 and {@code Long.MAX_VALUE}
 * respectively for all other partition modes.
 *
 * <h3>Partition modes (mutually exclusive, checked in order by {@link BamPartitionReader})</h3>
 * <ol>
 *   <li><b>Unmapped</b>: {@code queryUnmapped=true} → {@code samReader.queryUnmapped()}</li>
 *   <li><b>Per-reference</b>: {@code querySequences != null} → {@code samReader.query(intervals, false)}
 *       using BAI for VFO-based positioning</li>
 *   <li><b>Region push-down</b>: {@code querySequence != null} + {@code indexPath != null} →
 *       {@code samReader.query(ref, start, end, false)}</li>
 *   <li><b>Full scan</b>: fallback → {@code samReader.iterator()}</li>
 * </ol>
 *
 * <p>The Hadoop {@link Configuration} is carried per-partition so that executors
 * can reconstruct a {@link org.apache.hadoop.fs.FileSystem} with the correct
 * S3A/ADLS credentials even when they differ from the default Hadoop config.
 *
 * <p>CRAM-specific fields: {@code isCram} gates reference-source configuration in
 * {@link BamPartitionReader}. {@code referenceFile} is the path to the FASTA
 * reference (may be null). {@code referenceMode} is one of {@code "file"},
 * {@code "md5"}, or {@code "none"}.
 */
public class BamInputPartition implements InputPartition, Serializable {

    private final String path;
    private final long startByte;
    private final long endByte;
    private final SerializableConfiguration hadoopConf;
    /** Resolved BAI/CRAI path; null when no index is available. */
    private final String indexPath;
    private final boolean isCram;
    /** Path to FASTA reference file; null when not provided. Used for CRAM only. */
    private final String referenceFile;
    /** Reference resolution mode: "file", "md5", or "none". Used for CRAM only. */
    private final String referenceMode;
    /**
     * Reference sequence name for a BAI/CRAI-guided region push-down query; null means
     * use {@code querySequences} or full scan.  When set, the reader calls
     * {@code samReader.query(querySequence, queryStart, queryEnd, false)}.
     */
    private final String querySequence;
    private final int queryStart;
    private final int queryEnd;
    /**
     * One or more reference sequence names for VFO-based per-reference partitioning.
     * When non-null, each element is queried via the BAI index (which uses VFO chunks
     * internally for seeking). Multiple names are batched into a single
     * {@code QueryInterval[]} call. Null means fall through to {@code querySequence}
     * or full scan.
     */
    private final String[] querySequences;
    /**
     * When true, this partition reads only unplaced unmapped reads via
     * {@code samReader.queryUnmapped()}. Requires {@code indexPath} to be set.
     */
    private final boolean queryUnmapped;

    /** Full-file, no-index partition (used by existing tests and SAM files). */
    public BamInputPartition(String path,
                             long startVirtualOffset,
                             long endVirtualOffset,
                             Configuration hadoopConf) {
        this(path, startVirtualOffset, endVirtualOffset, hadoopConf, null);
    }

    public BamInputPartition(String path,
                             long startVirtualOffset,
                             long endVirtualOffset,
                             Configuration hadoopConf,
                             String indexPath) {
        this(path, startVirtualOffset, endVirtualOffset, hadoopConf, indexPath,
             false, null, "none");
    }

    public BamInputPartition(String path,
                             long startVirtualOffset,
                             long endVirtualOffset,
                             Configuration hadoopConf,
                             String indexPath,
                             boolean isCram,
                             String referenceFile,
                             String referenceMode) {
        this(path, startVirtualOffset, endVirtualOffset, hadoopConf, indexPath,
             isCram, referenceFile, referenceMode, null, 1, Integer.MAX_VALUE);
    }

    public BamInputPartition(String path,
                             long startVirtualOffset,
                             long endVirtualOffset,
                             Configuration hadoopConf,
                             String indexPath,
                             boolean isCram,
                             String referenceFile,
                             String referenceMode,
                             String querySequence,
                             int queryStart,
                             int queryEnd) {
        this(path, startVirtualOffset, endVirtualOffset, hadoopConf, indexPath,
             isCram, referenceFile, referenceMode, querySequence, queryStart, queryEnd,
             null, false);
    }

    /** Full constructor — includes VFO-splitting fields {@code querySequences} and {@code queryUnmapped}. */
    public BamInputPartition(String path,
                             long startVirtualOffset,
                             long endVirtualOffset,
                             Configuration hadoopConf,
                             String indexPath,
                             boolean isCram,
                             String referenceFile,
                             String referenceMode,
                             String querySequence,
                             int queryStart,
                             int queryEnd,
                             String[] querySequences,
                             boolean queryUnmapped) {
        this.path = path;
        this.startByte = startVirtualOffset;
        this.endByte = endVirtualOffset;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
        this.indexPath = indexPath;
        this.isCram = isCram;
        this.referenceFile = referenceFile;
        this.referenceMode = referenceMode;
        this.querySequence = querySequence;
        this.queryStart = queryStart;
        this.queryEnd = queryEnd;
        this.querySequences = querySequences;
        this.queryUnmapped = queryUnmapped;
    }

    public String getPath() { return path; }
    public long getStartByte() { return startByte; }
    public long getEndByte() { return endByte; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    public String getIndexPath() { return indexPath; }
    public boolean isCram() { return isCram; }
    public String getReferenceFile() { return referenceFile; }
    public String getReferenceMode() { return referenceMode; }
    public String getQuerySequence() { return querySequence; }
    public int getQueryStart() { return queryStart; }
    public int getQueryEnd() { return queryEnd; }
    /** Returns the reference names for per-reference VFO partitioning, or null for other modes. */
    public String[] getQuerySequences() { return querySequences; }
    /** Returns true if this partition should read only unplaced unmapped reads. */
    public boolean isQueryUnmapped() { return queryUnmapped; }
}
