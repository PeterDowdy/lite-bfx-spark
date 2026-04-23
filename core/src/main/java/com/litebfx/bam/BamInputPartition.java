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
 *   <li><b>SAM line-split</b>: {@code samSplit=true} → seeks to {@code startByte}, scans forward
 *       for the first non-header line, reads one line per record until {@code endByte}.</li>
 *   <li><b>BGZF split</b>: {@code startByte > 0} or {@code endByte != MAX_VALUE} (and not samSplit)
 *       → seeks to the first clean BGZF block boundary in the byte range and uses
 *       {@link htsjdk.samtools.BAMRecordCodec} directly.</li>
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
 *
 * <li><b>CRAM container-split</b>: {@code cramContainerSpans != null} → the reader creates a
 *     {@code CRAMIterator} over the specified container byte-offset spans and yields all records
 *     in those containers. Spans are alternating {@code [start₀, end₀, start₁, end₁, …]} pairs.</li>
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
    /**
     * When true, this partition uses SAM line-based splitting: the reader seeks to
     * {@code startByte}, discards bytes up to the next newline (landing on a clean
     * line boundary), then reads SAM text lines until the line-start position reaches
     * {@code endByte}. Distinct from BGZF split mode, which also uses
     * {@code startByte}/{@code endByte} but operates on compressed BAM blocks.
     */
    private final boolean samSplit;
    /**
     * Alternating {@code [start₀, end₀, start₁, end₁, …]} container byte-offset pairs for
     * CRAM container-split mode. When non-null, the reader creates a {@code CRAMIterator}
     * over these spans and yields all records in those containers. Null for all other modes.
     * An empty array (length 0) produces an empty partition.
     */
    private final long[] cramContainerSpans;

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
             null, false, false);
    }

    /** Full constructor — includes VFO-splitting fields {@code querySequences}, {@code queryUnmapped},
     *  {@code samSplit}, and CRAM container-split field {@code cramContainerSpans}. */
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
                             boolean queryUnmapped,
                             boolean samSplit) {
        this(path, startVirtualOffset, endVirtualOffset, hadoopConf, indexPath,
             isCram, referenceFile, referenceMode, querySequence, queryStart, queryEnd,
             querySequences, queryUnmapped, samSplit, null);
    }

    /** Full constructor with CRAM container-split support. */
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
                             boolean queryUnmapped,
                             boolean samSplit,
                             long[] cramContainerSpans) {
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
        this.samSplit = samSplit;
        this.cramContainerSpans = cramContainerSpans;
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
    /** Returns true if this partition uses SAM line-based splitting. */
    public boolean isSamSplit() { return samSplit; }
    /**
     * Returns the alternating {@code [start, end]} container byte-offset spans for CRAM
     * container-split mode, or {@code null} for all other partition modes.
     */
    public long[] getCramContainerSpans() { return cramContainerSpans; }
}
