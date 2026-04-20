package com.litebfx.bam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import com.litebfx.HadoopSeekableStream;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves BAM/SAM input files and plans {@link BamInputPartition}s.
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
 * <h3>VFO-based splitting (BAM + BAI, no pushed region)</h3>
 * When a BAI is available and no genomic region was pushed down by the query planner,
 * this scan creates one partition per reference sequence (or group of references when
 * {@code numPartitions} &lt; number of references), plus one partition for unplaced
 * unmapped reads.  Each per-reference reader calls {@code samReader.query()} with a
 * {@code QueryInterval[]}, causing htsjdk to use the BAI's virtual file offsets (VFOs)
 * to seek directly to each reference's data — no full-file scans.
 *
 * <h3>Region push-down (BAM + BAI + pushed region)</h3>
 * A single partition is planned with the pushed region; the reader calls
 * {@code samReader.query(ref, start, end, false)}.
 *
 * <h3>Fallback</h3>
 * SAM files and BAM files without a BAI always get a single full-scan partition.
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
        int maxPartitions = Integer.parseInt(options.getOrDefault("numPartitions", "200"));
        String referenceFile = options.get("referenceFile");
        String referenceMode = options.getOrDefault("referenceMode", referenceFile != null ? "file" : "none");
        log.trace("planInputPartitions() useIndex={} maxPartitions={} isCram={} referenceMode={}",
                useIndex, maxPartitions, isCram, referenceMode);

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        List<BamInputPartition> partitions = new ArrayList<>();
        try {
            for (Path filePath : resolveBamFiles(pathStr, hadoopConf)) {
                String fileUri = filePath.toUri().toString();
                String indexPath = useIndex ? resolveIndexPath(filePath, hadoopConf) : null;
                boolean isSam = fileUri.toLowerCase().endsWith(".sam");
                log.trace("planInputPartitions() fileUri={} indexPath={} isSam={}", fileUri, indexPath, isSam);

                if (!isCram && !isSam && indexPath != null && pushedReferenceName == null) {
                    // VFO-based per-reference splitting: one partition group per reference,
                    // plus one unmapped partition. htsjdk uses BAI VFOs internally.
                    planVfoPartitions(fileUri, filePath, indexPath, hadoopConf,
                            referenceFile, referenceMode, maxPartitions, partitions);
                } else if (!isCram && !isSam && indexPath == null && pushedReferenceName == null) {
                    // Unindexed BAM: BGZF block-level splitting into fixed-size chunks.
                    planBgzfSplitPartitions(fileUri, filePath, hadoopConf,
                            referenceFile, referenceMode, partitions);
                } else {
                    // Region push-down (single partition), SAM, or CRAM fallback.
                    String querySequence = (pushedReferenceName != null && indexPath != null)
                            ? pushedReferenceName : null;
                    int queryStart = querySequence != null ? pushedStart : 1;
                    int queryEnd   = querySequence != null ? pushedEnd   : Integer.MAX_VALUE;
                    log.trace("planInputPartitions() single partition querySequence={} queryStart={} queryEnd={}",
                            querySequence, queryStart, queryEnd);
                    partitions.add(new BamInputPartition(
                            fileUri, 0L, Long.MAX_VALUE, hadoopConf,
                            indexPath, isCram, referenceFile, referenceMode,
                            querySequence, queryStart, queryEnd));
                }
            }
        } catch (IOException e) {
            String fmt = isCram ? "CRAM" : "BAM";
            throw new RuntimeException("Failed to plan " + fmt + " input partitions for path: " + pathStr, e);
        }

        log.trace("planInputPartitions() -> {} partition(s)", partitions.size());
        return partitions.toArray(new InputPartition[0]);
    }

    /**
     * Plans per-reference VFO partitions for a BAM file with a BAI index.
     *
     * <p>Opens the BAM header to enumerate reference sequences, groups them into at most
     * {@code maxPartitions} partitions (each using a {@link QueryInterval}{@code []} so
     * htsjdk seeks to each reference's VFO chunks via the BAI), then appends one final
     * partition for unplaced unmapped reads.
     */
    private void planVfoPartitions(String fileUri,
                                   Path filePath,
                                   String indexPath,
                                   Configuration conf,
                                   String referenceFile,
                                   String referenceMode,
                                   int maxPartitions,
                                   List<BamInputPartition> out) throws IOException {
        log.trace("planVfoPartitions() fileUri={} maxPartitions={}", fileUri, maxPartitions);

        List<String> refs = readReferenceNames(filePath, new Path(indexPath), conf);
        log.trace("planVfoPartitions() {} references in header", refs.size());

        int numGroups = Math.max(1, Math.min(maxPartitions, refs.size()));
        List<List<String>> groups = groupRefs(refs, numGroups);

        for (List<String> group : groups) {
            String[] seqs = group.toArray(new String[0]);
            log.trace("planVfoPartitions() adding per-ref partition seqs={}", (Object) seqs);
            out.add(new BamInputPartition(
                    fileUri, 0L, Long.MAX_VALUE, conf,
                    indexPath, false, referenceFile, referenceMode,
                    null, 1, Integer.MAX_VALUE,
                    seqs, false));
        }

        // Unmapped partition: reads unplaced unmapped reads via samReader.queryUnmapped().
        log.trace("planVfoPartitions() adding unmapped partition");
        out.add(new BamInputPartition(
                fileUri, 0L, Long.MAX_VALUE, conf,
                indexPath, false, referenceFile, referenceMode,
                null, 1, Integer.MAX_VALUE,
                null, true));
    }

    /**
     * Plans fixed-size byte-range partitions for an unindexed BAM file.
     *
     * <p>Each executor will seek to its chunk boundary, scan forward for the first BGZF
     * block whose decompressed content starts at a clean BAM record boundary, and read
     * records from there until it reaches the next chunk boundary. Chunks that contain
     * no clean record start produce zero rows (safe — Spark unions all partitions).
     *
     * <p>The split size is controlled by the {@code bgzfSplitSize} option (default 128 MB).
     */
    private void planBgzfSplitPartitions(String fileUri,
                                         Path filePath,
                                         Configuration conf,
                                         String referenceFile,
                                         String referenceMode,
                                         List<BamInputPartition> out) throws IOException {
        long splitSize = Long.parseLong(options.getOrDefault("bgzfSplitSize", "134217728"));
        long fileSize = filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
        int numChunks = (int) Math.max(1, (long) Math.ceil((double) fileSize / splitSize));
        log.trace("planBgzfSplitPartitions() fileSize={} splitSize={} numChunks={}",
                fileSize, splitSize, numChunks);

        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * splitSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long)(i + 1) * splitSize;
            log.trace("planBgzfSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            out.add(new BamInputPartition(
                    fileUri, startByte, endByte, conf,
                    null, false, referenceFile, referenceMode,
                    null, 1, Integer.MAX_VALUE,
                    null, false));
        }
    }

    /**
     * Opens the BAM header (with its BAI) to retrieve all reference sequence names
     * in dictionary order.
     */
    private List<String> readReferenceNames(Path bamPath, Path baiPath, Configuration conf)
            throws IOException {
        FileSystem bamFs = bamPath.getFileSystem(conf);
        long bamLen = bamFs.getFileStatus(bamPath).getLen();
        FSDataInputStream bamStream = null;
        FSDataInputStream baiStream = null;
        try {
            bamStream = bamFs.open(bamPath);
            HadoopSeekableStream bamSeekable = new HadoopSeekableStream(
                    bamStream, bamLen, bamPath.toUri().toString());

            FileSystem baiFs = baiPath.getFileSystem(conf);
            long baiLen = baiFs.getFileStatus(baiPath).getLen();
            baiStream = baiFs.open(baiPath);
            HadoopSeekableStream baiSeekable = new HadoopSeekableStream(
                    baiStream, baiLen, baiPath.toUri().toString());

            SamReaderFactory factory = SamReaderFactory.makeDefault()
                    .validationStringency(ValidationStringency.LENIENT);
            List<String> refs = new ArrayList<>();
            try (SamReader reader = factory.open(
                    SamInputResource.of(bamSeekable).index(baiSeekable))) {
                SAMFileHeader header = reader.getFileHeader();
                for (SAMSequenceRecord seq : header.getSequenceDictionary().getSequences()) {
                    refs.add(seq.getSequenceName());
                }
            }
            return refs;
        } finally {
            // SamReader.close() closes the seekable wrappers, but close underlying streams too.
            if (bamStream != null) try { bamStream.close(); } catch (IOException e) {
                log.debug("suppressed exception closing BAM stream", e);
            }
            if (baiStream != null) try { baiStream.close(); } catch (IOException e) {
                log.debug("suppressed exception closing BAI stream", e);
            }
        }
    }

    /**
     * Distributes {@code refs} into {@code numGroups} roughly equal groups.
     * Groups are consecutive slices of the reference list (preserving dictionary order).
     */
    private static List<List<String>> groupRefs(List<String> refs, int numGroups) {
        List<List<String>> groups = new ArrayList<>(numGroups);
        int base = refs.size() / numGroups;
        int extra = refs.size() % numGroups;
        int idx = 0;
        for (int g = 0; g < numGroups; g++) {
            int size = base + (g < extra ? 1 : 0);
            groups.add(new ArrayList<>(refs.subList(idx, idx + size)));
            idx += size;
        }
        return groups;
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
