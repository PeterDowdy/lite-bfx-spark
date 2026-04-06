package com.litebfx.bam;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a BAM/SAM/CRAM file.
 *
 * <p>Virtual file offsets (VFOs) delimit the BGZF chunk range assigned to this
 * partition. For full-file (non-indexed) partitions, {@code startVirtualOffset}
 * is 0 and {@code endVirtualOffset} is {@link Long#MAX_VALUE}.
 *
 * <p>The Hadoop {@link Configuration} is carried per-partition so that executors
 * can reconstruct a {@link org.apache.hadoop.fs.FileSystem} with the correct
 * S3A/ADLS credentials even when they differ from the default Hadoop config.
 *
 * <p>{@code indexPath} records the resolved BAI/CRAI path for the file and will be
 * used by a future VFO-splitting implementation in {@link BamScan}.  The reader
 * does not open the index today — it always performs a full-file scan and relies
 * on Spark's post-scan filter pass for region correctness.
 *
 * <p>CRAM-specific fields: {@code isCram} gates reference-source configuration in
 * {@link BamPartitionReader}. {@code referenceFile} is the path to the FASTA
 * reference (may be null). {@code referenceMode} is one of {@code "file"},
 * {@code "md5"}, or {@code "none"}.
 */
public class BamInputPartition implements InputPartition, Serializable {

    private final String path;
    private final long startVirtualOffset;
    private final long endVirtualOffset;
    private final SerializableConfiguration hadoopConf;
    /** Resolved BAI/CRAI path; null when no index is available. Reserved for future VFO splitting. */
    private final String indexPath;
    private final boolean isCram;
    /** Path to FASTA reference file; null when not provided. Used for CRAM only. */
    private final String referenceFile;
    /** Reference resolution mode: "file", "md5", or "none". Used for CRAM only. */
    private final String referenceMode;

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
        this.path = path;
        this.startVirtualOffset = startVirtualOffset;
        this.endVirtualOffset = endVirtualOffset;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
        this.indexPath = indexPath;
        this.isCram = isCram;
        this.referenceFile = referenceFile;
        this.referenceMode = referenceMode;
    }

    public String getPath() { return path; }
    public long getStartVirtualOffset() { return startVirtualOffset; }
    public long getEndVirtualOffset() { return endVirtualOffset; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    public String getIndexPath() { return indexPath; }
    public boolean isCram() { return isCram; }
    public String getReferenceFile() { return referenceFile; }
    public String getReferenceMode() { return referenceMode; }
}
