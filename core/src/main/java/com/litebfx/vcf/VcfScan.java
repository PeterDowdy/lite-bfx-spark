package com.litebfx.vcf;

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
 * Plans one {@link VcfInputPartition} per VCF/BCF file (full-file or tabix region).
 *
 * <h3>Index resolution order (per VCF file)</h3>
 * <ol>
 *   <li>{@code indexPath} option</li>
 *   <li>Co-located {@code <vcfPath>.tbi}</li>
 *   <li>Co-located {@code <vcfPath>.csi}</li>
 *   <li>None → single partition, full-file scan</li>
 * </ol>
 *
 * <p>When a pushed {@code chrom} filter is present and a tabix index is found,
 * the partition is configured for a region query. Spark post-filters ensure
 * correctness for records near region boundaries.
 */
public class VcfScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(VcfScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final String pushedChrom;
    private final int pushedStart;
    private final int pushedEnd;

    VcfScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            String pushedChrom,
            int pushedStart,
            int pushedEnd) {
        this.options        = options;
        this.requiredSchema = requiredSchema;
        this.pushedChrom    = pushedChrom;
        this.pushedStart    = pushedStart;
        this.pushedEnd      = pushedEnd;
    }

    @Override
    public StructType readSchema() {
        return VcfSchema.SCHEMA;
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
            throw new IllegalArgumentException("'path' option is required for the vcf data source");
        }

        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));
        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            String indexPath = useIndex ? resolveIndexPath(pathStr, hadoopConf) : null;
            log.trace("planInputPartitions() indexPath={}", indexPath);

            // Only apply region query when we have both an index and a pushed chrom filter.
            String queryChrom = (indexPath != null && pushedChrom != null) ? pushedChrom : null;
            int    queryStart = queryChrom != null ? pushedStart : 1;
            int    queryEnd   = queryChrom != null ? pushedEnd   : Integer.MAX_VALUE;

            return new InputPartition[]{
                new VcfInputPartition(pathStr, indexPath, queryChrom, queryStart, queryEnd, hadoopConf)
            };
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan VCF input partitions for path: " + pathStr, e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new VcfPartitionReaderFactory();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the tabix index using the documented priority order.
     * Returns null when no index is found.
     */
    private String resolveIndexPath(String vcfPath, Configuration conf) throws IOException {
        // 1. Explicit indexPath option
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) {
                log.trace("resolveIndexPath() -> explicit indexPath={}", explicit);
                return explicit;
            }
        }

        FileSystem fs = new Path(vcfPath).getFileSystem(conf);

        // 2. Co-located <vcfPath>.tbi
        Path tbi = new Path(vcfPath + ".tbi");
        if (fs.exists(tbi)) {
            log.trace("resolveIndexPath() -> co-located .tbi={}", tbi);
            return tbi.toUri().toString();
        }

        // 3. Co-located <vcfPath>.csi
        Path csi = new Path(vcfPath + ".csi");
        if (fs.exists(csi)) {
            log.trace("resolveIndexPath() -> co-located .csi={}", csi);
            return csi.toUri().toString();
        }

        log.trace("resolveIndexPath() -> no index found");
        return null;
    }
}
