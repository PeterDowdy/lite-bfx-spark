package io.github.peterdowdy.litebfx.fasta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Plans one {@link FastaInputPartition} per contig when a FAI index is found,
 * or a single full-file partition when no index is available.
 *
 * <h3>FAI resolution order</h3>
 * <ol>
 *   <li>{@code indexPath} option</li>
 *   <li>Co-located {@code <fastaPath>.fai}</li>
 *   <li>None → single partition, full sequential scan</li>
 * </ol>
 */
public class FastaScan implements Scan, Batch, SupportsReportStatistics {

    private static final Logger log = LoggerFactory.getLogger(FastaScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    /** When non-null, only create a partition for the contig whose name matches this value. */
    private final String pushedName;
    /** Number of contigs in the FAI index; -1 when unknown (no FAI resolved at build time). */
    private final int faiEntryCount;
    private final int pushedLimit;
    private Statistics cachedStatistics = null;

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema) {
        this(options, requiredSchema, null, -1, Integer.MAX_VALUE);
    }

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema, String pushedName) {
        this(options, requiredSchema, pushedName, -1, Integer.MAX_VALUE);
    }

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema, String pushedName, int faiEntryCount) {
        this(options, requiredSchema, pushedName, faiEntryCount, Integer.MAX_VALUE);
    }

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema, String pushedName,
              int faiEntryCount, int pushedLimit) {
        log.trace("FastaScan(options={}, pushedName={}, faiEntryCount={}, pushedLimit={})",
                options, pushedName, faiEntryCount, pushedLimit);
        this.options = options;
        this.requiredSchema = requiredSchema;
        this.pushedName = pushedName;
        this.faiEntryCount = faiEntryCount;
        this.pushedLimit = pushedLimit;
    }

    @Override
    public StructType readSchema() {
        return io.github.peterdowdy.litebfx.FileMetadata.isRequested(requiredSchema)
                ? io.github.peterdowdy.litebfx.FileMetadata.withMetadata(FastaSchema.SCHEMA)
                : FastaSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the fasta data source");
        }

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            List<FileStatus> files = resolveFiles(options, hadoopConf);
            pathStr = files.get(0).getPath().toUri().toString();
            log.trace("planInputPartitions() path={}", pathStr);
            int maxMergeGap = FastaInputPartition.DEFAULT_MAX_MERGE_GAP;
            String maxMergeGapOpt = options.get("maxMergeGap");
            if (maxMergeGapOpt != null) {
                maxMergeGap = Integer.parseInt(maxMergeGapOpt);
                log.trace("planInputPartitions() maxMergeGap={}", maxMergeGap);
            }

            String faiPath = resolveFaiPath(options, pathStr, hadoopConf);
            if (faiPath != null) {
                List<String> contigNames = readContigNames(faiPath, hadoopConf);
                if (pushedName != null) {
                    contigNames = contigNames.stream()
                            .filter(pushedName::equals)
                            .collect(java.util.stream.Collectors.toList());
                    log.trace("planInputPartitions() name filter applied, {} matching contig(s)", contigNames.size());
                } else {
                    log.trace("planInputPartitions() FAI found, {} contig(s)", contigNames.size());
                }
                // Limit pushdown: if a limit is active and no name filter was pushed, plan only 1 contig.
                if (pushedLimit < Integer.MAX_VALUE && pushedName == null && contigNames.size() > 1) {
                    log.trace("planInputPartitions() limit={} -> trimming {} contig(s) -> 1", pushedLimit, contigNames.size());
                    contigNames = contigNames.subList(0, 1);
                }
                InputPartition[] partitions = new InputPartition[contigNames.size()];
                for (int i = 0; i < contigNames.size(); i++) {
                    int rowLimitForPartition = (pushedLimit < Integer.MAX_VALUE) ? pushedLimit : Integer.MAX_VALUE;
                    partitions[i] = new FastaInputPartition(pathStr, contigNames.get(i), faiPath,
                            hadoopConf, maxMergeGap, rowLimitForPartition);
                }
                return partitions;
            } else {
                log.trace("planInputPartitions() no FAI found, single full-scan partition");
                int rowLimitForPartition = (pushedLimit < Integer.MAX_VALUE) ? pushedLimit : Integer.MAX_VALUE;
                return new InputPartition[]{
                    new FastaInputPartition(pathStr, null, null, hadoopConf, maxMergeGap, rowLimitForPartition)
                };
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan FASTA input partitions for path: " + pathStr, e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.trace("createReaderFactory()");
        return new FastaPartitionReaderFactory(
                io.github.peterdowdy.litebfx.FileMetadata.isRequested(requiredSchema));
    }

    @Override
    public Statistics estimateStatistics() {
        if (cachedStatistics != null) return cachedStatistics;
        try {
            Configuration conf = SparkSession.builder().getOrCreate()
                    .sessionState().newHadoopConf();
            long total = 0;
            for (FileStatus fs : resolveFiles(options, conf)) {
                total += fs.getLen();
            }
            final long size = total;
            final OptionalLong rows = faiEntryCount >= 0
                    ? OptionalLong.of(faiEntryCount)
                    : OptionalLong.empty();
            cachedStatistics = new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.of(size); }
                public OptionalLong numRows()     { return rows; }
            };
        } catch (IOException e) {
            cachedStatistics = new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.empty(); }
                public OptionalLong numRows()     { return OptionalLong.empty(); }
            };
        }
        return cachedStatistics;
    }

    // -------------------------------------------------------------------------
    // File resolution
    // -------------------------------------------------------------------------

    static List<FileStatus> resolveFiles(CaseInsensitiveStringMap options, Configuration conf)
            throws IOException {
        String pathStr = options.get("path");
        if (pathStr == null) return new ArrayList<>();
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        List<FileStatus> result = new ArrayList<>();
        result.add(fs.getFileStatus(hadoopPath));
        return result;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static String resolveFaiPath(CaseInsensitiveStringMap options, String fastaPath, Configuration conf)
            throws IOException {
        // 1. Explicit indexPath option
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) {
                log.trace("resolveFaiPath() -> explicit indexPath={}", explicit);
                return explicit;
            }
        }

        // 2. Co-located <fastaPath>.fai
        Path colocated = new Path(fastaPath + ".fai");
        FileSystem fs = colocated.getFileSystem(conf);
        if (fs.exists(colocated)) {
            log.trace("resolveFaiPath() -> co-located fai={}", colocated);
            return colocated.toUri().toString();
        }

        log.trace("resolveFaiPath() -> no FAI found");
        return null;
    }

    /**
     * Parses contig names from a FAI file.
     * FAI format: NAME\tLENGTH\tOFFSET\tBASES_PER_LINE\tBYTES_PER_LINE
     */
    static List<String> readContigNames(String faiPath, Configuration conf) throws IOException {
        log.trace("readContigNames(faiPath={})", faiPath);
        Path path = new Path(faiPath);
        List<String> names = new ArrayList<>();
        try (FSDataInputStream in = path.getFileSystem(conf).open(path);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    names.add(line.split("\t", 2)[0]);
                }
            }
        }
        log.trace("readContigNames() -> {} contig(s)", names.size());
        return names;
    }
}
