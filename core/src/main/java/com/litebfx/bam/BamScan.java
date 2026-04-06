package com.litebfx.bam;

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
 * Resolves BAM/SAM input files and plans one {@link BamInputPartition} per file.
 *
 * <h3>File resolution</h3>
 * The {@code path} option may be a single file, a directory, or a glob.
 * {@code FileSystem.globStatus()} is used to expand it; directories are listed
 * shallowly and only {@code .bam}/{@code .sam} files are kept.
 *
 * <h3>BAI resolution order (per BAM file)</h3>
 * <ol>
 *   <li>{@code indexPath} option (single-file reads only)</li>
 *   <li>{@code indexDir/<filename>.bai}</li>
 *   <li>Co-located {@code <bamPath>.bai}</li>
 *   <li>None → full-file scan, no region query</li>
 * </ol>
 *
 * <h3>Region optimization</h3>
 * When a reference name (and optional start/end range) was extracted by
 * {@link BamScanBuilder} <em>and</em> a BAI index is available, the partition
 * carries the region so {@link BamPartitionReader} calls
 * {@code samReader.query()} instead of {@code samReader.iterator()}.
 * For SAM files and BAM files without a BAI the partition has no region and
 * a full scan is performed; Spark's post-scan filter ensures correctness.
 */
public class BamScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(BamScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final boolean includeAttributes;
    private final String pushedReferenceName;
    private final int pushedStart;
    private final int pushedEnd;
    private final boolean isCram;

    BamScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            boolean includeAttributes,
            String pushedReferenceName,
            int pushedStart,
            int pushedEnd,
            boolean isCram) {
        log.trace("BamScan(includeAttributes={}, pushedReferenceName={}, pushedStart={}, pushedEnd={}, isCram={})",
                includeAttributes, pushedReferenceName, pushedStart, pushedEnd, isCram);
        this.options = options;
        this.requiredSchema = requiredSchema;
        this.includeAttributes = includeAttributes;
        this.pushedReferenceName = pushedReferenceName;
        this.pushedStart = pushedStart;
        this.pushedEnd = pushedEnd;
        this.isCram = isCram;
    }

    // -------------------------------------------------------------------------
    // Scan
    // -------------------------------------------------------------------------

    @Override
    public StructType readSchema() {
        log.trace("readSchema()");
        // Always return the full schema. BAM records cannot be partially read,
        // so we always produce all 12 columns. Spark applies its own projection
        // on top when the user selects a subset of columns.
        // The only row-level optimization driven by pruneColumns is skipping
        // attributes map construction when "attributes" is not in the required schema.
        return BamSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        log.trace("toBatch()");
        return this;
    }

    // -------------------------------------------------------------------------
    // Batch
    // -------------------------------------------------------------------------

    @Override
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        log.trace("planInputPartitions() path={}", pathStr);
        if (pathStr == null) {
            String fmt = isCram ? "cram" : "bam";
            throw new IllegalArgumentException("'path' option is required for the " + fmt + " data source");
        }

        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));
        String referenceFile = options.get("referenceFile");
        String referenceMode = options.getOrDefault("referenceMode", referenceFile != null ? "file" : "none");
        log.trace("planInputPartitions() useIndex={} isCram={} referenceMode={}", useIndex, isCram, referenceMode);

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        List<BamInputPartition> partitions = new ArrayList<>();
        try {
            for (Path filePath : resolveBamFiles(pathStr, hadoopConf)) {
                String fileUri = filePath.toUri().toString();
                String indexPath = useIndex ? resolveIndexPath(filePath, hadoopConf) : null;
                log.trace("planInputPartitions() adding partition fileUri={} indexPath={}", fileUri, indexPath);

                partitions.add(new BamInputPartition(
                        fileUri,
                        0L,
                        Long.MAX_VALUE,
                        hadoopConf,
                        indexPath,
                        isCram,
                        referenceFile,
                        referenceMode));
            }
        } catch (IOException e) {
            String fmt = isCram ? "CRAM" : "BAM";
            throw new RuntimeException("Failed to plan " + fmt + " input partitions for path: " + pathStr, e);
        }

        log.trace("planInputPartitions() -> {} partition(s)", partitions.size());
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.trace("createReaderFactory()");
        return new BamPartitionReaderFactory(includeAttributes);
    }

    // -------------------------------------------------------------------------
    // File resolution helpers
    // -------------------------------------------------------------------------

    private List<Path> resolveBamFiles(String pathStr, Configuration conf) throws IOException {
        log.trace("resolveBamFiles(pathStr={})", pathStr);
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        List<Path> result = new ArrayList<>();

        FileStatus[] statuses = fs.globStatus(hadoopPath);
        if (statuses != null && statuses.length == 1 && statuses[0].isDirectory()) {
            collectBamChildren(fs, statuses[0].getPath(), result);
        } else if (statuses != null && statuses.length > 0) {
            for (FileStatus s : statuses) {
                if (s.isDirectory()) {
                    collectBamChildren(fs, s.getPath(), result);
                } else if (isAcceptedExtension(s.getPath())) {
                    result.add(s.getPath());
                }
            }
        }

        if (result.isEmpty()) {
            // Treat path as a literal file (lets htsjdk detect format by magic bytes)
            result.add(hadoopPath);
        }
        log.trace("resolveBamFiles() -> {} file(s)", result.size());
        return result;
    }

    private void collectBamChildren(FileSystem fs, Path dir, List<Path> result) throws IOException {
        log.trace("collectBamChildren(dir={})", dir);
        for (FileStatus s : fs.listStatus(dir)) {
            if (!s.isDirectory() && isAcceptedExtension(s.getPath())) {
                log.trace("collectBamChildren() found {}", s.getPath());
                result.add(s.getPath());
            }
        }
    }

    private boolean isAcceptedExtension(Path p) {
        String name = p.getName().toLowerCase();
        boolean result = isCram ? name.endsWith(".cram")
                                : name.endsWith(".bam") || name.endsWith(".sam");
        log.trace("isAcceptedExtension({}) -> {}", p.getName(), result);
        return result;
    }

    /**
     * Resolves the BAI (for BAM) or CRAI (for CRAM) index using the documented
     * priority order. Returns null when no index is found.
     */
    private String resolveIndexPath(Path filePath, Configuration conf) throws IOException {
        log.trace("resolveIndexPath(filePath={}, isCram={})", filePath, isCram);
        FileSystem fs = filePath.getFileSystem(conf);
        String indexSuffix = isCram ? ".crai" : ".bai";

        // 1. Explicit indexPath option (only reliable for single-file reads)
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) {
                log.trace("resolveIndexPath() -> explicit indexPath={}", explicit);
                return explicit;
            }
        }

        // 2. indexDir/<filename><suffix>
        String indexDir = options.get("indexDir");
        if (indexDir != null) {
            Path candidate = new Path(indexDir, filePath.getName() + indexSuffix);
            if (candidate.getFileSystem(conf).exists(candidate)) {
                log.trace("resolveIndexPath() -> indexDir candidate={}", candidate);
                return candidate.toUri().toString();
            }
        }

        // 3. Co-located <filePath><suffix>
        Path colocated = new Path(filePath.toUri().toString() + indexSuffix);
        if (fs.exists(colocated)) {
            log.trace("resolveIndexPath() -> co-located index={}", colocated);
            return colocated.toUri().toString();
        }

        log.trace("resolveIndexPath() -> no index found");
        return null;
    }
}
