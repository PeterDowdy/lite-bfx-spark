package com.litebfx.fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class FastqScan implements Batch, SupportsReportStatistics {

    private static final Logger log = LoggerFactory.getLogger(FastqScan.class);

    /** Default minimum split size for uncompressed FASTQ files (64 MiB). Overridable via {@code minSplitBytes} option. */
    static final long MIN_SPLIT_BYTES = 64L * 1024 * 1024;

    /** Default maximum number of partitions per file. */
    private static final int DEFAULT_NUM_PARTITIONS = 200;

    private final CaseInsensitiveStringMap options;
    private final int pushedLimit;

    FastqScan(CaseInsensitiveStringMap options) {
        this(options, Integer.MAX_VALUE);
    }

    FastqScan(CaseInsensitiveStringMap options, int pushedLimit) {
        log.trace("FastqScan(options={}, pushedLimit={})", options, pushedLimit);
        this.options = options;
        this.pushedLimit = pushedLimit;
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
    public Statistics estimateStatistics() {
        try {
            Configuration conf = SparkSession.builder().getOrCreate()
                    .sessionState().newHadoopConf();
            long total = 0;
            for (FileStatus fs : resolveFiles(options, conf)) {
                total += fs.getLen();
            }
            final long size = total;
            return new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.of(size); }
                public OptionalLong numRows()     { return OptionalLong.empty(); }
            };
        } catch (IOException e) {
            return new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.empty(); }
                public OptionalLong numRows()     { return OptionalLong.empty(); }
            };
        }
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
        long bgzfSplitSize = options.containsKey("bgzfSplitSize")
                ? Long.parseLong(options.get("bgzfSplitSize"))
                : 128L * 1024 * 1024; // 128 MiB default

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            List<InputPartition> partitions = new ArrayList<>();
            List<FileStatus> statuses = resolveFiles(options, hadoopConf);

            for (FileStatus status : statuses) {
                String filePath = status.getPath().toUri().toString();
                String filePathLower = filePath.toLowerCase();
                boolean isGzipped = filePathLower.endsWith(".fastq.gz")
                        || filePathLower.endsWith(".fq.gz");
                Integer readNumber = detectReadNumber(status.getPath().getName());

                if (isGzipped) {
                    long fileSize = status.getLen();
                    FileSystem fs = status.getPath().getFileSystem(hadoopConf);
                    if (fileSize > bgzfSplitSize && isBgzfFile(fs, status.getPath())) {
                        log.trace("planInputPartitions() BGZF detected -> split partitions for {}", filePath);
                        planBgzfSplitPartitions(filePath, fileSize, bgzfSplitSize, numPartitions,
                                hadoopConf, readNumber, partitions);
                    } else {
                        log.trace("planInputPartitions() gzipped -> single partition for {}", filePath);
                        partitions.add(new FastqInputPartition(filePath, 0L, Long.MAX_VALUE, hadoopConf, readNumber));
                    }
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

            // Limit pushdown: only the first partition is needed; its reader enforces rowLimit.
            if (pushedLimit < Integer.MAX_VALUE && !partitions.isEmpty()) {
                FastqInputPartition first = (FastqInputPartition) partitions.get(0);
                log.trace("planInputPartitions() limit={} -> trimming to 1 partition", pushedLimit);
                return new InputPartition[]{
                    new FastqInputPartition(first.getPath(), first.getStartByte(), first.getEndByte(),
                            first.getHadoopConf(), first.getReadNumber(), first.isBgzf(), pushedLimit)
                };
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

    /**
     * Plans fixed-size byte-range partitions for a BGZF-compressed FASTQ file.
     *
     * <p>Each partition covers a compressed-byte range. The reader seeks to the first
     * BGZF block at or after its {@code startByte}, scans forward in the decompressed
     * text to the next {@code @} record boundary, and stops when the BGZF block address
     * exceeds {@code endByte}.
     *
     * <p>The split size (in compressed bytes) is controlled by the {@code bgzfSplitSize}
     * option (default 128 MiB). The number of chunks is also capped at {@code numPartitions}.
     */
    private static void planBgzfSplitPartitions(String filePath,
                                                long fileSize,
                                                long splitSize,
                                                int maxPartitions,
                                                Configuration conf,
                                                Integer readNumber,
                                                List<InputPartition> out) {
        int numChunks = (int) Math.min(maxPartitions,
                Math.max(1L, (long) Math.ceil((double) fileSize / splitSize)));
        long chunkSize = fileSize / numChunks;
        log.trace("planBgzfSplitPartitions() fileSize={} numChunks={} chunkSize={}", fileSize, numChunks, chunkSize);
        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * chunkSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long)(i + 1) * chunkSize;
            log.trace("planBgzfSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            out.add(new FastqInputPartition(filePath, startByte, endByte, conf, readNumber, true));
        }
    }

    /**
     * Returns true when the file at {@code path} is BGZF-compressed.
     *
     * <p>Reads the first 16 bytes of the file and checks for the BGZF extra-field
     * subfield identifiers SI1={@code 'B'} (0x42) and SI2={@code 'C'} (0x43) at
     * the expected offsets in the gzip extra-field header.
     */
    private static boolean isBgzfFile(FileSystem fs, Path path) {
        byte[] header = new byte[16];
        try (FSDataInputStream in = fs.open(path)) {
            int n = 0;
            while (n < header.length) {
                int r = in.read(header, n, header.length - n);
                if (r < 0) return false;
                n += r;
            }
        } catch (IOException e) {
            return false;
        }
        // gzip magic (bytes 0-1), CM=deflate (byte 2), FLG has FEXTRA set (byte 3 bit 2),
        // BGZF extra subfield IDs SI1='B' (byte 12) and SI2='C' (byte 13).
        return header[0] == (byte) 0x1f
            && header[1] == (byte) 0x8b
            && header[2] == 0x08
            && (header[3] & 0x04) != 0
            && header[12] == 0x42   // 'B'
            && header[13] == 0x43;  // 'C'
    }

    static List<FileStatus> resolveFiles(CaseInsensitiveStringMap options, Configuration conf)
            throws IOException {
        String pathStr = options.get("path");
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(hadoopPath);
        if (status.isFile()) {
            List<FileStatus> result = new ArrayList<>();
            result.add(status);
            return result;
        }
        // Directory or glob
        FileStatus[] globbed = fs.globStatus(hadoopPath);
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
            if (!files.isEmpty()) return files;
        }
        // Fallback: list directory directly
        FileStatus[] listed = fs.listStatus(hadoopPath);
        List<FileStatus> files = new ArrayList<>();
        for (FileStatus s : listed) {
            if (s.isFile() && isFastqExtension(s.getPath().getName())) {
                files.add(s);
            }
        }
        return files;
    }

    private static boolean isFastqExtension(String name) {
        String lower = name.toLowerCase();
        return lower.endsWith(".fastq") || lower.endsWith(".fq")
                || lower.endsWith(".fastq.gz") || lower.endsWith(".fq.gz");
    }

    /**
     * Detects the read number (1 for R1, 2 for R2) from a FASTQ filename.
     *
     * <p>Recognised patterns (case-insensitive): {@code _(R?0*[12])[._]}
     * <ul>
     *   <li>{@code _R1_} / {@code _R2_} — Illumina BCL2FASTQ naming, e.g.
     *       {@code sample_R1_001.fastq.gz}</li>
     *   <li>{@code _R1.} / {@code _R2.} — simple suffix, e.g.
     *       {@code sample_R1.fastq.gz}</li>
     *   <li>{@code _1.} / {@code _2.} — alternative suffix, e.g.
     *       {@code sample_1.fastq.gz}</li>
     *   <li>{@code _01.} / {@code _02.} / {@code _R01_} — zero-padded variants, e.g.
     *       {@code sample_01.fastq.gz}</li>
     * </ul>
     *
     * @return 1, 2, or null when the read number cannot be determined
     */
    static Integer detectReadNumber(String filename) {
        Matcher m = READ_NUMBER_PATTERN.matcher(filename);
        if (!m.find()) return null;
        return Integer.parseInt(m.group(2));
    }

    /** Matches {@code _(R?0*[12])[._]} case-insensitively to detect R1/R2 read numbers. */
    private static final Pattern READ_NUMBER_PATTERN =
            Pattern.compile("_(R?0*([12]))[._]", Pattern.CASE_INSENSITIVE);
}
