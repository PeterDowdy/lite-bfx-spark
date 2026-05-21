package com.litebfx.vcf;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.tabix.TabixIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
 * Plans one or more {@link VcfInputPartition}s for a VCF/BCF file.
 *
 * <h3>Partitioning strategy</h3>
 * <ul>
 *   <li><b>Plain-text VCF</b> ({@code .vcf}): split into fixed-size byte-range chunks
 *       so multiple workers read in parallel. Each worker seeks to its chunk boundary,
 *       discards bytes up to the next newline, reads the {@code #CHROM} header line
 *       from offset 0 to obtain sample names, then parses data lines directly until
 *       its end boundary.  The split size is controlled by the {@code vcfSplitSize}
 *       option (default 128 MiB).  Any pushed region filter is applied by Spark as a
 *       post-filter on top of each partition's output.</li>
 *   <li><b>bgzipped or BCF with tabix, no region filter pushed</b> ({@code .vcf.gz},
 *       {@code .bcf}): the driver reads the tabix index to enumerate all chromosomes,
 *       groups them into at most {@code numPartitions} (default 200) groups, and
 *       creates one partition per group.  Each executor performs a tabix query for
 *       its assigned chromosomes — only the relevant BGZF blocks are read.</li>
 *   <li><b>bgzipped or BCF with tabix, region filter pushed</b>: single partition
 *       performing a targeted tabix region query for the pushed chromosome/range.</li>
 *   <li><b>bgzipped or BCF without index</b>: single partition, full-file scan.</li>
 * </ul>
 *
 * <h3>Index resolution order (bgzipped/BCF only)</h3>
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
            // Plain-text VCF: split into fixed-size byte-range chunks for parallel reads.
            if (isPlainTextVcf(pathStr)) {
                return planVcfSplitPartitions(pathStr, hadoopConf);
            }

            // Bgzipped VCF or BCF: per-chrom parallel partitions, region query, or full scan.
            String indexPath = useIndex ? resolveIndexPath(pathStr, hadoopConf) : null;
            log.trace("planInputPartitions() indexPath={}", indexPath);

            if (indexPath != null && pushedChrom == null) {
                // No region filter pushed: try to create per-chromosome partitions.
                List<String> chroms = readChromsFromTabix(indexPath, hadoopConf);
                if (chroms != null && !chroms.isEmpty()) {
                    return planTabixChromPartitions(pathStr, indexPath, chroms, hadoopConf);
                }
            }

            // Fallback: single partition — region query (if filter pushed) or full scan.
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
    // Split-partition planning
    // -------------------------------------------------------------------------

    /**
     * Plans fixed-size byte-range partitions for a plain-text VCF file.
     *
     * <p>Each executor seeks to its chunk boundary, discards bytes up to the next
     * newline to land on a clean line start, reads the {@code #CHROM} header from
     * offset 0 to extract sample names, and parses records until the next line
     * would begin at or past {@code endByte}.  Chunks that contain no data lines
     * produce zero rows.  Any pushed region filter is applied by Spark as a
     * post-filter on top of each partition's output.
     *
     * <p>The split size is controlled by the {@code vcfSplitSize} option (default 128 MiB).
     */
    private InputPartition[] planVcfSplitPartitions(String pathStr,
                                                    Configuration conf) throws IOException {
        long splitSize = Long.parseLong(options.getOrDefault("vcfSplitSize", "134217728"));
        if (splitSize <= 0) {
            throw new IllegalArgumentException(
                    "vcfSplitSize must be a positive integer, got: " + splitSize);
        }
        long fileSize = new Path(pathStr).getFileSystem(conf).getFileStatus(new Path(pathStr)).getLen();
        int numChunks = (int) Math.max(1, (long) Math.ceil((double) fileSize / splitSize));
        log.trace("planVcfSplitPartitions() fileSize={} splitSize={} numChunks={}", fileSize, splitSize, numChunks);

        List<VcfInputPartition> partitions = new ArrayList<>();
        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * splitSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long) (i + 1) * splitSize;
            log.trace("planVcfSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            partitions.add(new VcfInputPartition(
                    pathStr, null, null, 1, Integer.MAX_VALUE, startByte, endByte, conf));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    // -------------------------------------------------------------------------
    // Per-chromosome tabix partition planning
    // -------------------------------------------------------------------------

    /**
     * Plans one partition per chromosome group for a bgzipped VCF or BCF with a tabix index.
     *
     * <p>Chromosomes are distributed into at most {@code numPartitions} consecutive groups
     * (same algorithm as {@code BamScan.groupRefs}).  Each partition receives a
     * {@code String[]} of chromosome names; its reader chains one tabix query per chromosome.
     */
    private InputPartition[] planTabixChromPartitions(String pathStr,
                                                      String indexPath,
                                                      List<String> chroms,
                                                      Configuration conf) {
        int maxPartitions = Integer.parseInt(options.getOrDefault("numPartitions", "200"));
        if (maxPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + maxPartitions);
        }
        int numGroups = Math.max(1, Math.min(maxPartitions, chroms.size()));
        List<List<String>> groups = groupChroms(chroms, numGroups);
        log.trace("planTabixChromPartitions() chroms={} numGroups={}", chroms.size(), numGroups);

        List<VcfInputPartition> partitions = new ArrayList<>(groups.size());
        for (List<String> group : groups) {
            String[] arr = group.toArray(new String[0]);
            log.trace("planTabixChromPartitions() group={}", (Object) arr);
            partitions.add(new VcfInputPartition(pathStr, indexPath, arr, conf));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Reads the chromosome list from a BGZF-compressed tabix index via Hadoop FS.
     *
     * <p>The tabix index is small (typically &lt;1 MB) so downloading it on the driver is fast.
     * Returns {@code null} when the index cannot be opened, signalling that the caller should
     * fall back to a single full-file partition.
     */
    private List<String> readChromsFromTabix(String indexPath, Configuration conf) {
        try {
            Path hadoopPath = new Path(indexPath);
            FileSystem fs = hadoopPath.getFileSystem(conf);
            try (FSDataInputStream raw = fs.open(hadoopPath);
                 BlockCompressedInputStream bgzIn = new BlockCompressedInputStream(raw)) {
                List<String> chroms = new TabixIndex(bgzIn).getSequenceNames();
                log.trace("readChromsFromTabix() indexPath={} chroms={}", indexPath, chroms);
                return chroms;
            }
        } catch (Exception e) {
            log.warn("Could not read chromosomes from tabix index {}, falling back to single partition: {}",
                    indexPath, e.getMessage());
            return null;
        }
    }

    /**
     * Distributes {@code chroms} into {@code numGroups} roughly equal consecutive groups.
     * Mirrors the {@code groupRefs} algorithm in {@code BamScan}.
     */
    private static List<List<String>> groupChroms(List<String> chroms, int numGroups) {
        List<List<String>> groups = new ArrayList<>(numGroups);
        int base  = chroms.size() / numGroups;
        int extra = chroms.size() % numGroups;
        int idx   = 0;
        for (int g = 0; g < numGroups; g++) {
            int size = base + (g < extra ? 1 : 0);
            groups.add(new ArrayList<>(chroms.subList(idx, idx + size)));
            idx += size;
        }
        return groups;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns true when the path looks like a plain-text VCF file (not bgzipped or BCF).
     */
    private static boolean isPlainTextVcf(String path) {
        String lower = path.toLowerCase();
        return !lower.endsWith(".gz")
            && !lower.endsWith(".bgz")
            && !lower.endsWith(".bgzf")
            && !lower.endsWith(".bcf");
    }

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
