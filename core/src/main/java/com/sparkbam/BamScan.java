package com.litebfx;

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

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final boolean includeAttributes;
    private final String pushedReferenceName;
    private final int pushedStart;
    private final int pushedEnd;

    BamScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            boolean includeAttributes,
            String pushedReferenceName,
            int pushedStart,
            int pushedEnd) {
        this.options = options;
        this.requiredSchema = requiredSchema;
        this.includeAttributes = includeAttributes;
        this.pushedReferenceName = pushedReferenceName;
        this.pushedStart = pushedStart;
        this.pushedEnd = pushedEnd;
    }

    // -------------------------------------------------------------------------
    // Scan
    // -------------------------------------------------------------------------

    @Override
    public StructType readSchema() {
        // Always return the full schema. BAM records cannot be partially read,
        // so we always produce all 12 columns. Spark applies its own projection
        // on top when the user selects a subset of columns.
        // The only row-level optimization driven by pruneColumns is skipping
        // attributes map construction when "attributes" is not in the required schema.
        return BamSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    // -------------------------------------------------------------------------
    // Batch
    // -------------------------------------------------------------------------

    @Override
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the bam data source");
        }

        boolean useIndex = Boolean.parseBoolean(options.getOrDefault("useIndex", "true"));

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        List<BamInputPartition> partitions = new ArrayList<>();
        try {
            for (Path bamPath : resolveBamFiles(pathStr, hadoopConf)) {
                String bamUri = bamPath.toUri().toString();
                String indexPath = useIndex ? resolveIndexPath(bamPath, hadoopConf) : null;

                partitions.add(new BamInputPartition(
                        bamUri,
                        0L,
                        Long.MAX_VALUE,
                        hadoopConf,
                        indexPath));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan BAM input partitions for path: " + pathStr, e);
        }

        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new BamPartitionReaderFactory(includeAttributes);
    }

    // -------------------------------------------------------------------------
    // File resolution helpers
    // -------------------------------------------------------------------------

    private List<Path> resolveBamFiles(String pathStr, Configuration conf) throws IOException {
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
                } else if (isBamOrSam(s.getPath())) {
                    result.add(s.getPath());
                }
            }
        }

        if (result.isEmpty()) {
            // Treat path as a literal file (lets htsjdk detect BAM vs SAM by magic bytes)
            result.add(hadoopPath);
        }
        return result;
    }

    private void collectBamChildren(FileSystem fs, Path dir, List<Path> result) throws IOException {
        for (FileStatus s : fs.listStatus(dir)) {
            if (!s.isDirectory() && isBamOrSam(s.getPath())) {
                result.add(s.getPath());
            }
        }
    }

    private static boolean isBamOrSam(Path p) {
        String name = p.getName().toLowerCase();
        return name.endsWith(".bam") || name.endsWith(".sam");
    }

    /**
     * Resolves the BAI index for {@code bamPath} using the documented priority order.
     * Returns null when no index is found.
     */
    private String resolveIndexPath(Path bamPath, Configuration conf) throws IOException {
        FileSystem fs = bamPath.getFileSystem(conf);

        // 1. Explicit indexPath option (only reliable for single-file reads)
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) return explicit;
        }

        // 2. indexDir/<filename>.bai
        String indexDir = options.get("indexDir");
        if (indexDir != null) {
            Path candidate = new Path(indexDir, bamPath.getName() + ".bai");
            if (candidate.getFileSystem(conf).exists(candidate)) {
                return candidate.toUri().toString();
            }
        }

        // 3. Co-located <bamPath>.bai
        Path colocated = new Path(bamPath.toUri().toString() + ".bai");
        if (fs.exists(colocated)) return colocated.toUri().toString();

        return null;
    }
}
