package com.litebfx.fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans one or more {@link FastqInputPartition}s for a FASTQ file.
 *
 * <h3>Partitioning strategy</h3>
 * <ul>
 *   <li><b>Gzipped files</b> ({@code .fastq.gz} / {@code .fq.gz}): single partition
 *       spanning the whole file. Gzip is not seekable.</li>
 *   <li><b>Uncompressed files</b>: the file is divided into at most
 *       {@code numPartitions} byte-range splits of at least {@value #MIN_SPLIT_BYTES}
 *       bytes each. Each {@link FastqPartitionReader} seeks to its split start and
 *       advances to the next {@code @} record boundary before reading.</li>
 * </ul>
 */
public class FastqScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(FastqScan.class);

    /** Default minimum split size for uncompressed FASTQ files (64 MiB). Overridable via {@code minSplitBytes} option. */
    static final long MIN_SPLIT_BYTES = 64L * 1024 * 1024;

    /** Default maximum number of partitions per file. */
    private static final int DEFAULT_NUM_PARTITIONS = 200;

    private final CaseInsensitiveStringMap options;

    FastqScan(CaseInsensitiveStringMap options) {
        log.trace("FastqScan(options={})", options);
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return FastqSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        log.trace("planInputPartitions() path={}", pathStr);
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the fastq data source");
        }

        int numPartitions = options.containsKey("numPartitions")
                ? Integer.parseInt(options.get("numPartitions"))
                : DEFAULT_NUM_PARTITIONS;
        long minSplitBytes = options.containsKey("minSplitBytes")
                ? Long.parseLong(options.get("minSplitBytes"))
                : MIN_SPLIT_BYTES;

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            List<InputPartition> partitions = new ArrayList<>();
            Path hadoopPath = new Path(pathStr);
            FileSystem fs = hadoopPath.getFileSystem(hadoopConf);
            FileStatus[] statuses = resolveFiles(fs, hadoopPath);

            for (FileStatus status : statuses) {
                String filePath = status.getPath().toUri().toString();
                String filePathLower = filePath.toLowerCase();
                boolean isGzipped = filePathLower.endsWith(".fastq.gz")
                        || filePathLower.endsWith(".fq.gz");
                Integer readNumber = detectReadNumber(status.getPath().getName());

                if (isGzipped) {
                    log.trace("planInputPartitions() gzipped -> single partition for {}", filePath);
                    partitions.add(new FastqInputPartition(filePath, 0L, Long.MAX_VALUE, hadoopConf, readNumber));
                } else {
                    long fileSize = status.getLen();
                    int splits = (int) Math.min(numPartitions,
                            Math.max(1L, fileSize / minSplitBytes));
                    long splitSize = fileSize / splits;
                    log.trace("planInputPartitions() uncompressed size={} -> {} splits for {}",
                            fileSize, splits, filePath);
                    for (int i = 0; i < splits; i++) {
                        long start = i * splitSize;
                        long end = (i == splits - 1) ? Long.MAX_VALUE : ((i + 1) * splitSize);
                        partitions.add(new FastqInputPartition(filePath, start, end, hadoopConf, readNumber));
                    }
                }
            }

            log.trace("planInputPartitions() -> {} partition(s)", partitions.size());
            return partitions.toArray(new InputPartition[0]);

        } catch (IOException e) {
            throw new RuntimeException("Failed to plan FASTQ input partitions for path: " + pathStr, e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.trace("createReaderFactory()");
        return new FastqPartitionReaderFactory();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static FileStatus[] resolveFiles(FileSystem fs, Path path) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        if (status.isFile()) {
            return new FileStatus[]{status};
        }
        // Directory or glob
        FileStatus[] globbed = fs.globStatus(path);
        if (globbed != null && globbed.length > 0) {
            List<FileStatus> files = new ArrayList<>();
            for (FileStatus s : globbed) {
                if (s.isFile() && isFastqExtension(s.getPath().getName())) {
                    files.add(s);
                } else if (s.isDirectory()) {
                    for (FileStatus child : fs.listStatus(s.getPath())) {
                        if (child.isFile() && isFastqExtension(child.getPath().getName())) {
                            files.add(child);
                        }
                    }
                }
            }
            if (!files.isEmpty()) return files.toArray(new FileStatus[0]);
        }
        // Fallback: list directory directly
        FileStatus[] listed = fs.listStatus(path);
        List<FileStatus> files = new ArrayList<>();
        for (FileStatus s : listed) {
            if (s.isFile() && isFastqExtension(s.getPath().getName())) {
                files.add(s);
            }
        }
        return files.toArray(new FileStatus[0]);
    }

    private static boolean isFastqExtension(String name) {
        String lower = name.toLowerCase();
        return lower.endsWith(".fastq") || lower.endsWith(".fq")
                || lower.endsWith(".fastq.gz") || lower.endsWith(".fq.gz");
    }

    /**
     * Detects the read number (1 for R1, 2 for R2) from a FASTQ filename.
     *
     * <p>Recognised patterns (case-insensitive):
     * <ul>
     *   <li>{@code _R1_} / {@code _R2_} — Illumina BCL2FASTQ naming, e.g.
     *       {@code sample_R1_001.fastq.gz}</li>
     *   <li>{@code _R1.} / {@code _R2.} — simple suffix, e.g.
     *       {@code sample_R1.fastq.gz}</li>
     *   <li>{@code _1.} / {@code _2.} — alternative suffix, e.g.
     *       {@code sample_1.fastq.gz}</li>
     * </ul>
     *
     * @return 1, 2, or null when the read number cannot be determined
     */
    static Integer detectReadNumber(String filename) {
        String lower = filename.toLowerCase();
        if (lower.contains("_r1_") || lower.contains("_r1.")) return 1;
        if (lower.contains("_r2_") || lower.contains("_r2.")) return 2;
        // Alternative: _1. / _2. patterns (e.g. sample_1.fastq.gz)
        if (lower.contains("_1.")) return 1;
        if (lower.contains("_2.")) return 2;
        return null;
    }
}
