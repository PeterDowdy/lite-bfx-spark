package com.litebfx.bed;

import org.apache.hadoop.conf.Configuration;
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

/**
 * Plans one {@link BedInputPartition} per BED file (full-file or tabix region).
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
public class BedScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(BedScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final String pushedChrom;
    private final long pushedStart;
    private final long pushedEnd;

    BedScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            String pushedChrom,
            long pushedStart,
            long pushedEnd) {
        this.options        = options;
        this.requiredSchema = requiredSchema;
        this.pushedChrom    = pushedChrom;
        this.pushedStart    = pushedStart;
        this.pushedEnd      = pushedEnd;
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
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        log.trace("planInputPartitions() path={}", pathStr);
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the bed data source");
        }

        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));
        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            String indexPath = useIndex ? resolveIndexPath(pathStr, hadoopConf) : null;
            log.trace("planInputPartitions() indexPath={}", indexPath);

            // Determine query parameters: only apply region query when we have
            // both a tabix index and a pushed chromosome filter.
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

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new BedPartitionReaderFactory();
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
