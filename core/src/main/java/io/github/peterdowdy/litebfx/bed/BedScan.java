package io.github.peterdowdy.litebfx.bed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportOrdering;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Plans {@link BedInputPartition}s for a BED file: byte-range splits for plain-text, tabix region queries for bgzipped.
 *
 * <h3>Index resolution order (per BED file)</h3>
 * <ol>
 *   <li>{@code indexPath} option</li>
 *   <li>Co-located {@code <bedPath>.tbi}</li>
 *   <li>Co-located {@code <bedPath>.csi}</li>
 *   <li>None → single partition, full-file scan</li>
 * </ol>
 *
 * <p>When a pushed {@code chrom} filter is present and a tabix index is found,
 * the partition is configured for a region query. Spark post-filters ensure
 * correctness for records near region boundaries.
 */
public class BedScan implements Scan, Batch, SupportsReportStatistics, SupportsReportOrdering {

    private static final Logger log = LoggerFactory.getLogger(BedScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final String pushedChrom;
    private final long pushedStart;
    private final long pushedEnd;
    private final int pushedLimit;
    private Statistics cachedStatistics = null;
    private SortOrder[] cachedOrdering = null;

    BedScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            String pushedChrom,
            long pushedStart,
            long pushedEnd) {
        this(options, requiredSchema, pushedChrom, pushedStart, pushedEnd, Integer.MAX_VALUE);
    }

    BedScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            String pushedChrom,
            long pushedStart,
            long pushedEnd,
            int pushedLimit) {
        this.options        = options;
        this.requiredSchema = requiredSchema;
        this.pushedChrom    = pushedChrom;
        this.pushedStart    = pushedStart;
        this.pushedEnd      = pushedEnd;
        this.pushedLimit    = pushedLimit;
    }

    @Override
    public StructType readSchema() {
        return BedSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return this;
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
            cachedStatistics = new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.of(size); }
                public OptionalLong numRows()     { return OptionalLong.empty(); }
            };
        } catch (IOException e) {
            cachedStatistics = new Statistics() {
                public OptionalLong sizeInBytes() { return OptionalLong.empty(); }
                public OptionalLong numRows()     { return OptionalLong.empty(); }
            };
        }
        return cachedStatistics;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions = doplanInputPartitions();
        if (pushedLimit < Integer.MAX_VALUE && pushedChrom == null && partitions.length > 0) {
            log.trace("planInputPartitions() limit={} -> trimming {} -> 1 partition(s)",
                    pushedLimit, partitions.length);
            return new InputPartition[]{
                ((BedInputPartition) partitions[0]).withRowLimit(pushedLimit)
            };
        }
        return partitions;
    }

    private InputPartition[] doplanInputPartitions() {
        String pathStr = options.get("path");
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the bed data source");
        }

        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));
        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            List<FileStatus> files = resolveFiles(options, hadoopConf);
            pathStr = files.get(0).getPath().toUri().toString();
            log.trace("planInputPartitions() path={}", pathStr);
            // Plain-text BED: split into fixed-size byte-range chunks so multiple
            // workers read in parallel. Each worker seeks to its chunk boundary,
            // discards bytes up to the next newline, and reads records until its
            // end boundary. A region filter (pushedChrom) is applied as a Spark
            // post-filter on top of each partition's output.
            boolean isCompressed = pathStr.toLowerCase().endsWith(".gz")
                    || pathStr.toLowerCase().endsWith(".bgz")
                    || pathStr.toLowerCase().endsWith(".bgzf");
            if (!isCompressed) {
                return planBedSplitPartitions(pathStr, hadoopConf);
            }

            // Bgzipped BED: tabix region query or single full-file scan.
            String indexPath = useIndex ? resolveIndexPath(pathStr, hadoopConf) : null;
            log.trace("planInputPartitions() indexPath={}", indexPath);

            String queryChrom = (indexPath != null && pushedChrom != null) ? pushedChrom : null;
            long   queryStart = queryChrom != null ? pushedStart : 0L;
            long   queryEnd   = queryChrom != null ? pushedEnd   : Long.MAX_VALUE;

            return new InputPartition[]{
                new BedInputPartition(pathStr, indexPath, queryChrom, queryStart, queryEnd, hadoopConf)
            };
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan BED input partitions for path: " + pathStr, e);
        }
    }

    /**
     * Plans fixed-size byte-range partitions for a plain-text BED file.
     *
     * <p>Each executor seeks to its chunk boundary, discards bytes up to the next
     * newline to land on a clean line start, and reads records until the next line
     * would begin at or past {@code endByte}.  Chunks that contain no data lines
     * produce zero rows.  Any pushed region filter is applied by Spark as a
     * post-filter on top of each partition's output.
     *
     * <p>The split size is controlled by the {@code bedSplitSize} option (default 128 MB).
     */
    private InputPartition[] planBedSplitPartitions(String pathStr,
                                                    Configuration conf) throws IOException {
        long splitSize = Long.parseLong(options.getOrDefault("bedSplitSize", "134217728"));
        if (splitSize <= 0) {
            throw new IllegalArgumentException(
                    "bedSplitSize must be a positive integer, got: " + splitSize);
        }
        long fileSize = new Path(pathStr).getFileSystem(conf).getFileStatus(new Path(pathStr)).getLen();
        int numChunks = (int) Math.max(1, (long) Math.ceil((double) fileSize / splitSize));
        log.trace("planBedSplitPartitions() fileSize={} splitSize={} numChunks={}", fileSize, splitSize, numChunks);

        List<BedInputPartition> partitions = new ArrayList<>();
        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * splitSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long) (i + 1) * splitSize;
            log.trace("planBedSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            partitions.add(new BedInputPartition(
                    pathStr, null, null, 0L, Long.MAX_VALUE, startByte, endByte, conf));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new BedPartitionReaderFactory();
    }

    @Override
    public SortOrder[] outputOrdering() {
        if (cachedOrdering != null) return cachedOrdering;
        String path = options.getOrDefault("path", "");
        boolean isCompressed = path.toLowerCase().endsWith(".gz")
                || path.toLowerCase().endsWith(".bgz")
                || path.toLowerCase().endsWith(".bgzf");
        if (!isCompressed) return cachedOrdering = new SortOrder[0];
        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));
        if (!useIndex) return cachedOrdering = new SortOrder[0];
        try {
            Configuration conf = SparkSession.builder().getOrCreate()
                    .sessionState().newHadoopConf();
            if (resolveIndexPath(path, conf) == null) return cachedOrdering = new SortOrder[0];
        } catch (Exception e) {
            return cachedOrdering = new SortOrder[0];
        }
        return cachedOrdering = new SortOrder[]{
            SortValue.apply(FieldReference.apply("chrom"),      SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
            SortValue.apply(FieldReference.apply("chromStart"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST)
        };
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

    /**
     * Resolves the tabix index using the documented priority order.
     * Returns null when no index is found.
     */
    private String resolveIndexPath(String bedPath, Configuration conf) throws IOException {
        // 1. Explicit indexPath option
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) {
                log.trace("resolveIndexPath() -> explicit indexPath={}", explicit);
                return explicit;
            }
        }

        FileSystem fs = new Path(bedPath).getFileSystem(conf);

        // 2. Co-located <bedPath>.tbi
        Path tbi = new Path(bedPath + ".tbi");
        if (fs.exists(tbi)) {
            log.trace("resolveIndexPath() -> co-located .tbi={}", tbi);
            return tbi.toUri().toString();
        }

        // 3. Co-located <bedPath>.csi
        Path csi = new Path(bedPath + ".csi");
        if (fs.exists(csi)) {
            log.trace("resolveIndexPath() -> co-located .csi={}", csi);
            return csi.toUri().toString();
        }

        log.trace("resolveIndexPath() -> no index found");
        return null;
    }
}
