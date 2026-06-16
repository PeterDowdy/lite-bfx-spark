package io.github.peterdowdy.litebfx.fasta;

import io.github.peterdowdy.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.HasPartitionStatistics;

import java.util.OptionalLong;

/**
 * Describes a single Spark partition over a FASTA file.
 *
 * <p>When a FAI index is present, one partition is created per contig and
 * {@code contigName} is set to the contig identifier; the reader uses
 * {@code IndexedFastaSequenceFile.getSequence(contigName)} for O(1) access.
 *
 * <p>When no FAI is available, a single partition is created with
 * {@code contigName=null}; the reader iterates all sequences via
 * {@code nextSequence()}.
 */
public class FastaInputPartition implements HasPartitionStatistics {

    private final String path;
    /** Contig to read; null means "iterate all contigs in order". */
    private final String contigName;
    /**
     * Path to the FAI index file, or null when no index is available.
     * Required on executors for non-local (e.g. S3A) indexed reads so that
     * {@link FastaPartitionReader} can seek to the correct byte offset.
     */
    private final String faiPath;
    private final SerializableConfiguration hadoopConf;
    /**
     * Maximum gap in bytes between consecutive base lines that will be bridged
     * in a single contiguous Range request. When {@code newlineSize <=
     * maxMergeGap}, the reader issues one seek and reads all raw bytes (bases +
     * inter-line newlines) in a single call, then strips newlines in memory.
     * When {@code newlineSize > maxMergeGap} each line is fetched individually.
     *
     * <p>Standard FASTA newlines are 1 byte (LF) or 2 bytes (CRLF), so the
     * default of {@value #DEFAULT_MAX_MERGE_GAP} bytes collapses them for all
     * real-world FASTA files.  Set to {@code 0} to disable collapsing.
     */
    public static final int DEFAULT_MAX_MERGE_GAP = 128;
    private final int maxMergeGap;
    /** Maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    private final int rowLimit;

    public FastaInputPartition(String path, String contigName, String faiPath,
                               Configuration hadoopConf) {
        this(path, contigName, faiPath, hadoopConf, DEFAULT_MAX_MERGE_GAP, Integer.MAX_VALUE);
    }

    public FastaInputPartition(String path, String contigName, String faiPath,
                               Configuration hadoopConf, int maxMergeGap) {
        this(path, contigName, faiPath, hadoopConf, maxMergeGap, Integer.MAX_VALUE);
    }

    public FastaInputPartition(String path, String contigName, String faiPath,
                               Configuration hadoopConf, int maxMergeGap, int rowLimit) {
        this.path = path;
        this.contigName = contigName;
        this.faiPath = faiPath;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
        this.maxMergeGap = maxMergeGap;
        this.rowLimit = rowLimit;
    }

    public String getPath() { return path; }
    public String getContigName() { return contigName; }
    public String getFaiPath() { return faiPath; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
    public int getMaxMergeGap() { return maxMergeGap; }
    /** Returns the maximum number of rows to return; Integer.MAX_VALUE means no limit. */
    public int getRowLimit() { return rowLimit; }

    // FASTA partitions are per-contig; byte size is not known without reading the FAI entry.
    @Override public OptionalLong sizeInBytes() { return OptionalLong.empty(); }
    @Override public OptionalLong numRows()     { return OptionalLong.empty(); }
    @Override public OptionalLong filesCount()  { return OptionalLong.of(1L); }
}
