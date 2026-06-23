package io.github.peterdowdy.litebfx.bam;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import io.github.peterdowdy.litebfx.HadoopSeekableStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import htsjdk.samtools.cram.CRAIEntry;
import htsjdk.samtools.cram.build.CramContainerHeaderIterator;
import htsjdk.samtools.cram.structure.Container;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

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
 * <h3>Hybrid indexed splitting (BAM + BAI, no pushed region)</h3>
 * When a BAI is available and no genomic region was pushed down, this scan creates one partition
 * per reference, but any reference whose data span exceeds {@code indexedSplitSize} (default 128 MB)
 * is further divided into multiple partitions. Split boundaries are <b>record-start virtual file
 * offsets</b> taken from the BAI (chunk starts and linear-index entries), so each partition seeks
 * straight to a real record boundary and the per-reference union is exact — no records dropped or
 * duplicated. One partition for unplaced unmapped reads is appended. When the file has more
 * references than {@code numPartitions}, falls back to reference-grouped VFO partitions
 * ({@link #planVfoPartitions}).
 *
 * <h3>Region push-down (BAM + BAI + pushed region)</h3>
 * The pushed region's BAI span is likewise divided into {@code indexedSplitSize}-sized VFO splits,
 * so a large region read is parallelised across partitions while still skipping bytes outside it.
 *
 * <h3>Fallback</h3>
 * SAM files and BAM files without a BAI always get a single full-scan partition. CRAM uses its own
 * CRAI/container splitting.
 */
public class BamScan implements Scan, Batch, SupportsReportStatistics, SupportsReportOrdering {

    private static final Logger log = LoggerFactory.getLogger(BamScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    private final boolean includeAttributes;
    private final String pushedReferenceName;
    private final int pushedStart;
    private final int pushedEnd;
    private final boolean isCram;
    private final int pushedLimit;
    private final boolean includeFileMetadata;

    private boolean headerRead = false;
    private boolean isCoordinateSorted = false;
    private Statistics cachedStatistics = null;
    private SortOrder[] cachedOrdering = null;

    BamScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            boolean includeAttributes,
            String pushedReferenceName,
            int pushedStart,
            int pushedEnd,
            boolean isCram) {
        this(options, requiredSchema, includeAttributes, pushedReferenceName,
             pushedStart, pushedEnd, isCram, Integer.MAX_VALUE);
    }

    BamScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            boolean includeAttributes,
            String pushedReferenceName,
            int pushedStart,
            int pushedEnd,
            boolean isCram,
            int pushedLimit) {
        this(options, requiredSchema, includeAttributes, pushedReferenceName,
             pushedStart, pushedEnd, isCram, pushedLimit, false);
    }

    BamScan(CaseInsensitiveStringMap options,
            StructType requiredSchema,
            boolean includeAttributes,
            String pushedReferenceName,
            int pushedStart,
            int pushedEnd,
            boolean isCram,
            int pushedLimit,
            boolean includeFileMetadata) {
        log.trace("BamScan(incAttr={}, ref={}, start={}, end={}, cram={}, limit={}, fileMeta={})",
                includeAttributes, pushedReferenceName, pushedStart, pushedEnd, isCram, pushedLimit,
                includeFileMetadata);
        this.options = options;
        this.requiredSchema = requiredSchema;
        this.includeAttributes = includeAttributes;
        this.pushedReferenceName = pushedReferenceName;
        this.pushedStart = pushedStart;
        this.pushedEnd = pushedEnd;
        this.isCram = isCram;
        this.pushedLimit = pushedLimit;
        this.includeFileMetadata = includeFileMetadata;
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
        StructType schema = BamSchema.fromOptions(options);
        // Append the hidden _metadata column when it was referenced (see SupportsMetadataColumns);
        // the reader produces its value positionally after the data columns.
        if (includeFileMetadata) {
            schema = io.github.peterdowdy.litebfx.FileMetadata.withMetadata(schema);
        }
        return schema;
    }

    @Override
    public Batch toBatch() {
        log.trace("toBatch()");
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
    public SortOrder[] outputOrdering() {
        if (cachedOrdering != null) return cachedOrdering;
        Configuration conf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();
        ensureHeaderRead(conf);
        if (!isCoordinateSorted) {
            cachedOrdering = new SortOrder[0];
        } else {
            // Use whichever column names the active schema exposes (descriptive vs. SAM-spec),
            // otherwise the reported ordering references columns that do not exist in SAM mode.
            boolean sam = BamSchema.isSamColumnNames(options);
            String refCol = sam ? "rname" : "referenceName";
            String startCol = sam ? "pos" : "start";
            cachedOrdering = new SortOrder[]{
                SortValue.apply(FieldReference.apply(refCol),   SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
                SortValue.apply(FieldReference.apply(startCol), SortDirection.ASCENDING, NullOrdering.NULLS_LAST)
            };
        }
        return cachedOrdering;
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
        // Hybrid indexed splitting: a single reference (or pushed region) whose BAI byte span
        // exceeds this size is divided into multiple BGZF byte-range partitions. Default 128 MB.
        long indexedSplitSize = Long.parseLong(options.getOrDefault("indexedSplitSize", "134217728"));
        if (indexedSplitSize <= 0) {
            throw new IllegalArgumentException(
                    "indexedSplitSize must be a positive integer, got: " + indexedSplitSize);
        }
        String referenceFile = options.get("referenceFile");
        String referenceMode = options.getOrDefault("referenceMode", referenceFile != null ? "file" : "none");
        log.trace("planInputPartitions() useIndex={} maxPartitions={} isCram={} referenceMode={}",
                useIndex, maxPartitions, isCram, referenceMode);

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();
        ensureHeaderRead(hadoopConf);

        List<BamInputPartition> partitions = new ArrayList<>();
        try {
            for (FileStatus fileStatus : resolveFiles(options, hadoopConf)) {
                Path filePath = fileStatus.getPath();
                String fileUri = filePath.toUri().toString();
                String indexPath = useIndex ? resolveIndexPath(filePath, hadoopConf) : null;
                boolean isSam = fileUri.toLowerCase().endsWith(".sam");
                // Hybrid VFO splitting reads the BAI on the driver to compute split offsets — only
                // worthwhile when the file is larger than one split. For smaller files no reference
                // or region can exceed indexedSplitSize, so use the cheaper index-guided paths that
                // don't touch the BAI on the driver.
                boolean hybridSplittable = fileStatus.getLen() > indexedSplitSize;
                log.trace("planInputPartitions() fileUri={} indexPath={} isSam={} hybridSplittable={}",
                        fileUri, indexPath, isSam, hybridSplittable);

                if (!isCram && !isSam && indexPath != null && pushedReferenceName == null
                        && hybridSplittable) {
                    // Hybrid per-reference splitting: one partition per reference, but any
                    // reference whose BAI byte span exceeds indexedSplitSize is further divided
                    // into balanced VFO partitions. Plus one unmapped partition.
                    planIndexedSplitPartitions(fileUri, filePath, indexPath, hadoopConf,
                            referenceFile, referenceMode, maxPartitions, indexedSplitSize, partitions);
                } else if (!isCram && !isSam && indexPath != null && pushedReferenceName == null) {
                    // Indexed BAM that fits in one split: per-reference VFO partitions (no driver BAI read).
                    planVfoPartitions(fileUri, filePath, indexPath, hadoopConf,
                            referenceFile, referenceMode, maxPartitions, partitions);
                } else if (!isCram && !isSam && indexPath == null && pushedReferenceName == null) {
                    // Unindexed BAM: BGZF block-level splitting into fixed-size chunks.
                    planBgzfSplitPartitions(fileUri, filePath, hadoopConf,
                            referenceFile, referenceMode, partitions);
                } else if (isSam) {
                    // SAM: plain-text line-based splitting into fixed-size chunks.
                    // When a region filter is pushed, each worker applies it independently —
                    // there is no index to skip I/O, but we still get parallel reads.
                    planSamSplitPartitions(fileUri, filePath, hadoopConf, partitions);
                } else if (isCram && pushedReferenceName == null) {
                    // CRAM: container-level splitting via CRAI index (if available) or header scan.
                    if (indexPath != null) {
                        planCraiPartitions(fileUri, filePath, indexPath, hadoopConf,
                                referenceFile, referenceMode, maxPartitions, partitions);
                    } else {
                        planCramContainerSplitPartitions(fileUri, filePath, hadoopConf,
                                referenceFile, referenceMode, maxPartitions, partitions);
                    }
                } else {
                    // Region push-down with BAI/CRAI.
                    String querySequence = (pushedReferenceName != null && indexPath != null)
                            ? pushedReferenceName : null;
                    int queryStart = querySequence != null ? pushedStart : 1;
                    int queryEnd   = querySequence != null ? pushedEnd   : Integer.MAX_VALUE;
                    log.trace("planInputPartitions() region querySequence={} queryStart={} queryEnd={}",
                            querySequence, queryStart, queryEnd);
                    if (querySequence != null && !isCram && hybridSplittable) {
                        // Hybrid: split the pushed region's BAI byte span into balanced partitions
                        // (skips bytes outside the region and parallelises a large region read).
                        planIndexedRegionSplitPartitions(fileUri, filePath, indexPath, hadoopConf,
                                referenceFile, referenceMode, querySequence, queryStart, queryEnd,
                                indexedSplitSize, partitions);
                    } else if (querySequence != null) {
                        // Small file or CRAM: a single index-guided region-query partition
                        // (no driver-side BAI read).
                        partitions.add(BamInputPartition.forRegionQuery(
                                fileUri, hadoopConf, indexPath, isCram,
                                referenceFile, referenceMode,
                                querySequence, queryStart, queryEnd));
                    } else {
                        partitions.add(BamInputPartition.forFullScan(
                                fileUri, hadoopConf, indexPath, isCram,
                                referenceFile, referenceMode));
                    }
                }
            }
        } catch (IOException e) {
            String fmt = isCram ? "CRAM" : "BAM";
            throw new RuntimeException("Failed to plan " + fmt + " input partitions for path: " + pathStr, e);
        }

        // Limit pushdown: when no region was pushed and a limit is active, keep only the first
        // one or two partitions (first reference group + unmapped) and apply rowLimit.
        if (pushedLimit < Integer.MAX_VALUE && pushedReferenceName == null && !partitions.isEmpty()) {
            int keep = Math.min(2, partitions.size());
            List<BamInputPartition> limited = new ArrayList<>(keep);
            for (int i = 0; i < keep; i++) {
                limited.add(partitions.get(i).withRowLimit(pushedLimit));
            }
            log.trace("planInputPartitions() limit={} -> trimming {} -> {} partition(s)",
                    pushedLimit, partitions.size(), limited.size());
            return limited.toArray(new InputPartition[0]);
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
            out.add(BamInputPartition.forVfoPartitions(
                    fileUri, conf, indexPath, referenceFile, referenceMode, seqs));
        }

        // Unmapped partition: reads unplaced unmapped reads via samReader.queryUnmapped().
        log.trace("planVfoPartitions() adding unmapped partition");
        out.add(BamInputPartition.forUnmapped(
                fileUri, conf, indexPath, referenceFile, referenceMode));
    }

    /**
     * Hybrid per-reference splitting for an indexed BAM with no region filter.
     *
     * <p>Opens the BAM+BAI once to read the header and {@link BAMIndex}. Each reference becomes
     * one partition, except references whose BAI byte span exceeds {@code splitSize}, which are
     * divided into multiple balanced VFO-split partitions (see {@link #addReferenceVfoSplits}).
     * One unmapped partition is appended. When the file has more references than
     * {@code maxPartitions} (so the count must be capped by grouping), falls back to the
     * reference-grouped {@link #planVfoPartitions}.
     */
    private void planIndexedSplitPartitions(String fileUri,
                                            Path filePath,
                                            String indexPath,
                                            Configuration conf,
                                            String referenceFile,
                                            String referenceMode,
                                            int maxPartitions,
                                            long splitSize,
                                            List<BamInputPartition> out) throws IOException {
        List<String> refs = readReferenceNames(filePath, new Path(indexPath), conf);
        if (refs.size() > maxPartitions) {
            log.trace("planIndexedSplitPartitions() {} refs > maxPartitions {} — falling back to VFO grouping",
                    refs.size(), maxPartitions);
            planVfoPartitions(fileUri, filePath, indexPath, conf,
                    referenceFile, referenceMode, maxPartitions, out);
            return;
        }

        withBamIndex(filePath, new Path(indexPath), conf, (header, index) -> {
            for (int refIdx = 0; refIdx < refs.size(); refIdx++) {
                int seqLen = header.getSequence(refIdx).getSequenceLength();
                addReferenceVfoSplits(index, header, refIdx, refs.get(refIdx),
                        1, seqLen, splitSize,
                        fileUri, conf, indexPath, referenceFile, referenceMode, out);
            }
        });

        log.trace("planIndexedSplitPartitions() adding unmapped partition");
        out.add(BamInputPartition.forUnmapped(fileUri, conf, indexPath, referenceFile, referenceMode));
    }

    /**
     * Hybrid splitting for a pushed-down region query. Divides the BAI byte span overlapping
     * {@code [queryStart, queryEnd]} on {@code refName} into balanced BGZF byte-range partitions,
     * skipping bytes outside the region. Falls back to a single region-query partition when the
     * reference is absent or has no indexed data.
     */
    private void planIndexedRegionSplitPartitions(String fileUri,
                                                  Path filePath,
                                                  String indexPath,
                                                  Configuration conf,
                                                  String referenceFile,
                                                  String referenceMode,
                                                  String refName,
                                                  int queryStart,
                                                  int queryEnd,
                                                  long splitSize,
                                                  List<BamInputPartition> out) throws IOException {
        int before = out.size();
        withBamIndex(filePath, new Path(indexPath), conf, (header, index) -> {
            int refIdx = header.getSequenceIndex(refName);
            if (refIdx < 0) return; // reference not in this file — leave empty, handled below
            addReferenceVfoSplits(index, header, refIdx, refName,
                    queryStart, queryEnd, splitSize,
                    fileUri, conf, indexPath, referenceFile, referenceMode, out);
        });
        if (out.size() == before) {
            // Reference absent or no indexed data: a single region query yields the correct
            // (possibly empty) result.
            out.add(BamInputPartition.forRegionQuery(fileUri, conf, indexPath, false,
                    referenceFile, referenceMode, refName, queryStart, queryEnd));
        }
    }

    /**
     * Appends BAI-guided VFO-split partitions covering one reference's data in
     * {@code [queryStart, queryEnd]}. Split boundaries are <b>record-start virtual file offsets</b>
     * taken from the BAI (chunk starts and linear-index entries), so each partition seeks straight
     * to a real record boundary — no byte-offset/block-boundary guessing, and the union across
     * splits is exact. The reference's data span is divided into bins of roughly {@code splitSize}
     * compressed bytes; a reference (or region) that fits in one bin yields a single partition.
     * References with no indexed data in the range contribute nothing.
     */
    private void addReferenceVfoSplits(BAMIndex index,
                                       SAMFileHeader header,
                                       int refIdx,
                                       String refName,
                                       int queryStart,
                                       int queryEnd,
                                       long splitSize,
                                       String fileUri,
                                       Configuration conf,
                                       String indexPath,
                                       String referenceFile,
                                       String referenceMode,
                                       List<BamInputPartition> out) {
        // Clamp the query interval to the reference length: getSpanOverlapping expects a 1-based
        // inclusive end, and an unbounded pushed end (Integer.MAX_VALUE) yields a truncated span.
        int seqLen = header.getSequence(refIdx).getSequenceLength();
        int effStart = Math.max(1, queryStart);
        int effEnd = (queryEnd <= 0 || queryEnd > seqLen) ? seqLen : queryEnd;

        BAMFileSpan whole = index.getSpanOverlapping(refIdx, effStart, effEnd);
        // getSpanOverlapping returns null (not an empty span) for a reference with no overlapping reads.
        List<Chunk> chunks = whole == null ? java.util.Collections.emptyList() : whole.getChunks();
        if (chunks.isEmpty()) {
            log.trace("addReferenceVfoSplits() ref={} no indexed data in [{},{}]", refName, effStart, effEnd);
            return;
        }
        long startVfo = chunks.get(0).getChunkStart();
        long endVfo   = chunks.get(chunks.size() - 1).getChunkEnd();
        long spanBytes = Math.max(1, (endVfo >> 16) - (startVfo >> 16));

        if (spanBytes <= splitSize) {
            out.add(BamInputPartition.forIndexedVfoSplit(fileUri, conf, indexPath, startVfo, endVfo,
                    refName, queryStart, queryEnd, referenceFile, referenceMode));
            return;
        }

        int targetSplits = (int) Math.min(Integer.MAX_VALUE, (spanBytes + splitSize - 1) / splitSize);
        // Harvest candidate record-start VFOs across the queried coordinate range. The BAI linear
        // index resolves a coordinate to the VFO of the first record overlapping it (<=16 kbp
        // granularity); oversampling 4x relative to targetSplits yields enough cut points to balance
        // bins by compressed bytes.
        TreeSet<Long> cutVfos = new TreeSet<>();
        cutVfos.add(startVfo);
        long coordStep = Math.max(1L, (long) (effEnd - effStart) / ((long) targetSplits * 4L));
        for (long c = effStart; c <= effEnd; c += coordStep) {
            int cc = (int) Math.min(Integer.MAX_VALUE, c);
            BAMFileSpan s = index.getSpanOverlapping(refIdx, cc, cc);
            if (s != null && !s.getChunks().isEmpty()) {
                cutVfos.add(s.getChunks().get(0).getChunkStart());
            }
        }

        // Group consecutive cut VFOs into bins of ~splitSize compressed bytes; the final bin runs to
        // the reference's end VFO. Boundaries are exact record starts, so bins tile [startVfo, endVfo)
        // with no gaps or overlaps.
        long binStartVfo = startVfo;
        int emitted = 0;
        for (long cut : cutVfos) {
            if (cut <= binStartVfo || cut >= endVfo) continue;
            if (((cut >> 16) - (binStartVfo >> 16)) >= splitSize) {
                out.add(BamInputPartition.forIndexedVfoSplit(fileUri, conf, indexPath, binStartVfo, cut,
                        refName, queryStart, queryEnd, referenceFile, referenceMode));
                binStartVfo = cut;
                emitted++;
            }
        }
        out.add(BamInputPartition.forIndexedVfoSplit(fileUri, conf, indexPath, binStartVfo, endVfo,
                refName, queryStart, queryEnd, referenceFile, referenceMode));
        log.trace("addReferenceVfoSplits() ref={} span={}B -> {} partition(s)", refName, spanBytes, emitted + 1);
    }

    /** Functional callback over an opened {@link SAMFileHeader} + {@link BAMIndex}. */
    @FunctionalInterface
    private interface IndexAction {
        void apply(SAMFileHeader header, BAMIndex index) throws IOException;
    }

    /**
     * Opens the BAM together with its BAI (sharing the {@link #readReferenceNames} open pattern),
     * exposes the header and {@link BAMIndex} to {@code action}, and closes everything afterward.
     */
    private void withBamIndex(Path bamPath, Path baiPath, Configuration conf, IndexAction action)
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
            try (SamReader reader = factory.open(
                    SamInputResource.of(bamSeekable).index(baiSeekable))) {
                action.apply(reader.getFileHeader(), reader.indexing().getIndex());
            }
        } finally {
            if (bamStream != null) try { bamStream.close(); } catch (IOException e) {
                log.debug("suppressed exception closing BAM stream", e);
            }
            if (baiStream != null) try { baiStream.close(); } catch (IOException e) {
                log.debug("suppressed exception closing BAI stream", e);
            }
        }
    }

    /**
     * Plans fixed-size byte-range partitions for an unindexed BAM file.
     *
     * <p>Each executor seeks to its chunk boundary, locates the first BAM record that starts at or
     * after it, and reads records until it reaches the next chunk boundary. Chunks that contain no
     * record start produce zero rows (safe — Spark unions all partitions).
     *
     * <h3>Why splits are not BGZF-block-aligned</h3>
     * <p>Split boundaries are computed arithmetically ({@code i × splitSize}) rather than
     * by enumerating actual BGZF block offsets. This is intentional: enumerating block
     * boundaries would require the driver to scan the entire file sequentially — thousands
     * of seeks on cloud storage (S3, GCS, ADLS) — just to build a partition plan.
     *
     * <p>Instead, each executor orients itself locally: {@link BamPartitionReader#guessFirstRecordVfo}
     * walks the contiguous BGZF blocks from the chunk boundary and finds the first record start —
     * which usually lies <em>within</em> a block, since the writer flushes blocks mid-record. A
     * candidate is accepted only when it and the following few records all parse as self-consistent
     * BAM records, so the split is lossless even when every block begins mid-record. This work is
     * bounded and negligible relative to the data each partition reads at the default 128 MB split.
     *
     * <h3>Degenerate split sizes</h3>
     * <p>Setting {@code bgzfSplitSize} smaller than {@code MAX_COMPRESSED_BLOCK_SIZE}
     * (~65 KB) will produce many partitions that do a ~65 KB scan only to find no clean
     * record start and yield zero rows. At very small values (e.g. 1 byte, as used in
     * correctness tests) the scan overhead dominates. There is no correctness risk, but
     * throughput degrades. The option exists for testing and for files with unusually
     * large records; the default (128 MB) is appropriate for typical WGS BAM files.
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
        if (splitSize <= 0) {
            throw new IllegalArgumentException(
                    "bgzfSplitSize must be a positive integer, got: " + splitSize);
        }
        long fileSize = filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
        int numChunks = (int) Math.max(1, (long) Math.ceil((double) fileSize / splitSize));
        log.trace("planBgzfSplitPartitions() fileSize={} splitSize={} numChunks={}",
                fileSize, splitSize, numChunks);

        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * splitSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long)(i + 1) * splitSize;
            log.trace("planBgzfSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            out.add(BamInputPartition.forBgzfSplit(
                    fileUri, conf, startByte, endByte, referenceFile, referenceMode));
        }
    }

    /**
     * Plans fixed-size byte-range partitions for a SAM file.
     *
     * <p>SAM is plain text with no BGZF framing, so blocks cannot be located by magic bytes.
     * Instead each executor seeks to its chunk boundary, discards bytes up to the next newline
     * to land on a clean line start, and reads SAM text lines (parsed via
     * {@link htsjdk.samtools.SAMLineParser}) until the next line would begin at or past
     * {@code endByte}. Chunks that contain no data lines produce zero rows.
     *
     * <p>The split size is controlled by the {@code samSplitSize} option (default 128 MB).
     */
    private void planSamSplitPartitions(String fileUri,
                                        Path filePath,
                                        Configuration conf,
                                        List<BamInputPartition> out) throws IOException {
        long splitSize = Long.parseLong(options.getOrDefault("samSplitSize", "134217728"));
        if (splitSize <= 0) {
            throw new IllegalArgumentException(
                    "samSplitSize must be a positive integer, got: " + splitSize);
        }
        long fileSize = filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
        int numChunks = (int) Math.max(1, (long) Math.ceil((double) fileSize / splitSize));
        log.trace("planSamSplitPartitions() fileSize={} splitSize={} numChunks={}",
                fileSize, splitSize, numChunks);

        for (int i = 0; i < numChunks; i++) {
            long startByte = (long) i * splitSize;
            long endByte   = (i == numChunks - 1) ? Long.MAX_VALUE : (long) (i + 1) * splitSize;
            log.trace("planSamSplitPartitions() chunk={} startByte={} endByte={}", i, startByte, endByte);
            out.add(BamInputPartition.forSamSplit(fileUri, conf, startByte, endByte));
        }
    }

    /**
     * Plans container-level partitions for a CRAM file that has a CRAI index.
     *
     * <p>Parses the CRAI (a gzip-compressed tab-separated text file) to collect the unique
     * container byte offsets.  Multiple CRAI entries that share the same container offset
     * (slices within one container) are deduplicated via a {@link TreeSet}.  The sorted
     * offsets are then distributed into at most {@code maxPartitions} groups; each group
     * becomes one {@link BamInputPartition} whose reader will use {@code CRAMIterator} with
     * the {@code [start, end]} byte-span pair for its assigned containers.
     */
    private void planCraiPartitions(String fileUri,
                                     Path filePath,
                                     String indexPath,
                                     Configuration conf,
                                     String referenceFile,
                                     String referenceMode,
                                     int maxPartitions,
                                     List<BamInputPartition> out) throws IOException {
        log.trace("planCraiPartitions() fileUri={} indexPath={}", fileUri, indexPath);

        TreeSet<Long> seen = new TreeSet<>();
        Path craiPath = new Path(indexPath);
        FileSystem craiFs = craiPath.getFileSystem(conf);
        try (FSDataInputStream craiStream = craiFs.open(craiPath);
             GZIPInputStream gzis = new GZIPInputStream(craiStream);
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzis))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    seen.add(new CRAIEntry(line).getContainerStartByteOffset());
                } catch (Exception e) {
                    log.debug("planCraiPartitions() skipping unparseable CRAI line: {}", line);
                }
            }
        }

        if (seen.isEmpty()) {
            log.trace("planCraiPartitions() no containers in CRAI — single full-scan partition");
            out.add(BamInputPartition.forFullScan(
                    fileUri, conf, indexPath, true, referenceFile, referenceMode));
            return;
        }

        List<Long> offsets = new ArrayList<>(seen);
        long fileSize = filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
        log.trace("planCraiPartitions() {} unique containers, fileSize={}", offsets.size(), fileSize);

        addCramContainerPartitions(fileUri, offsets, fileSize, conf, indexPath,
                referenceFile, referenceMode, maxPartitions, out);
    }

    /**
     * Plans container-level partitions for a CRAM file without a CRAI index.
     *
     * <p>Uses {@link CramContainerHeaderIterator} to scan container headers sequentially
     * (no record decoding), collecting each container's byte offset.  The offsets are then
     * distributed across at most {@code maxPartitions} groups exactly as in
     * {@link #planCraiPartitions}.
     */
    private void planCramContainerSplitPartitions(String fileUri,
                                                   Path filePath,
                                                   Configuration conf,
                                                   String referenceFile,
                                                   String referenceMode,
                                                   int maxPartitions,
                                                   List<BamInputPartition> out) throws IOException {
        log.trace("planCramContainerSplitPartitions() fileUri={}", fileUri);

        FileSystem cramFs = filePath.getFileSystem(conf);
        long fileSize = cramFs.getFileStatus(filePath).getLen();
        List<Long> offsets = new ArrayList<>();
        try (FSDataInputStream cramStream = cramFs.open(filePath);
             CramContainerHeaderIterator iter = new CramContainerHeaderIterator(cramStream)) {
            while (iter.hasNext()) {
                Container container = iter.next();
                if (!container.isEOF()) {
                    offsets.add(container.getContainerByteOffset());
                }
            }
        }

        if (offsets.isEmpty()) {
            log.trace("planCramContainerSplitPartitions() no containers — single full-scan partition");
            out.add(BamInputPartition.forFullScan(
                    fileUri, conf, null, true, referenceFile, referenceMode));
            return;
        }

        log.trace("planCramContainerSplitPartitions() {} containers, fileSize={}", offsets.size(), fileSize);
        addCramContainerPartitions(fileUri, offsets, fileSize, conf, null,
                referenceFile, referenceMode, maxPartitions, out);
    }

    /**
     * Groups a sorted list of CRAM container byte offsets into at most {@code maxPartitions}
     * partitions and appends a {@link BamInputPartition} for each group.
     * Each partition carries a two-element {@code cramContainerSpans} array
     * {@code [groupStart, groupEnd]} that the reader passes to {@code CRAMIterator}.
     */
    private void addCramContainerPartitions(String fileUri,
                                             List<Long> offsets,
                                             long fileSize,
                                             Configuration conf,
                                             String indexPath,
                                             String referenceFile,
                                             String referenceMode,
                                             int maxPartitions,
                                             List<BamInputPartition> out) {
        int n = offsets.size();
        int numGroups = Math.max(1, Math.min(maxPartitions, n));
        int base  = n / numGroups;
        int extra = n % numGroups;
        int idx = 0;
        for (int g = 0; g < numGroups; g++) {
            int size = base + (g < extra ? 1 : 0);
            // CramSpanContainerIterator.Boundary uses VFO encoding (rawByteOffset << 16).
            // Boundary.hasNext() checks: streamPosition <= end >>> 16 (inclusive).
            // After reading a container, position advances to the START of the next container.
            // So for non-last partitions we use (nextContainerStart - 1) << 16 as the
            // exclusive-end sentinel to prevent spilling into the next partition.
            // For the last partition, fileSize << 16 is fine: CRAMIterator stops on EOF container.
            long groupStart = offsets.get(idx) << 16;
            long groupEnd   = (idx + size < n)
                    ? (offsets.get(idx + size) - 1) << 16
                    : fileSize << 16;
            long[] spans = new long[]{groupStart, groupEnd};
            log.trace("addCramContainerPartitions() group={} spans=[{}, {}]", g, groupStart, groupEnd);
            out.add(BamInputPartition.forCramContainerSplit(
                    fileUri, conf, indexPath, referenceFile, referenceMode, spans));
            idx += size;
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
        return new BamPartitionReaderFactory(includeAttributes, includeFileMetadata);
    }

    // -------------------------------------------------------------------------
    // Header helpers
    // -------------------------------------------------------------------------

    /**
     * Lazily opens the first resolved file, reads the {@link SAMFileHeader}, and sets
     * {@link #isCoordinateSorted}. Idempotent — subsequent calls are no-ops.
     */
    private void ensureHeaderRead(Configuration conf) {
        if (headerRead) return;
        headerRead = true;
        try {
            List<FileStatus> files = resolveFiles(options, conf);
            if (files.isEmpty()) return;
            Path filePath = files.get(0).getPath();
            // No index → full sequential scan, no global ordering guarantee.
            boolean hasIndex = resolveIndexPath(filePath, conf) != null;
            FileSystem fs = filePath.getFileSystem(conf);
            long fileLen = fs.getFileStatus(filePath).getLen();
            try (FSDataInputStream stream = fs.open(filePath)) {
                HadoopSeekableStream seekable = new HadoopSeekableStream(
                        stream, fileLen, filePath.toUri().toString());
                SamReaderFactory factory = SamReaderFactory.makeDefault()
                        .validationStringency(ValidationStringency.LENIENT);
                try (SamReader reader = factory.open(SamInputResource.of(seekable))) {
                    SAMFileHeader header = reader.getFileHeader();
                    isCoordinateSorted = hasIndex &&
                            header.getSortOrder() == SAMFileHeader.SortOrder.coordinate;
                    log.trace("ensureHeaderRead() sortOrder={} hasIndex={} isCoordinateSorted={}",
                            header.getSortOrder(), hasIndex, isCoordinateSorted);
                }
            }
        } catch (IOException e) {
            log.debug("ensureHeaderRead() failed to read header", e);
        }
    }

    // -------------------------------------------------------------------------
    // File resolution helpers
    // -------------------------------------------------------------------------

    List<FileStatus> resolveFiles(CaseInsensitiveStringMap options, Configuration conf) throws IOException {
        String pathStr = options.get("path");
        log.trace("resolveFiles(pathStr={})", pathStr);
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        List<FileStatus> result = new ArrayList<>();

        FileStatus[] statuses = fs.globStatus(hadoopPath);
        if (statuses != null && statuses.length == 1 && statuses[0].isDirectory()) {
            collectBamChildren(fs, statuses[0].getPath(), result);
        } else if (statuses != null && statuses.length > 0) {
            for (FileStatus s : statuses) {
                if (s.isDirectory()) {
                    collectBamChildren(fs, s.getPath(), result);
                } else if (isAcceptedExtension(s.getPath())) {
                    result.add(s);
                }
            }
        }

        if (result.isEmpty()) {
            // Treat path as a literal file (lets htsjdk detect format by magic bytes)
            result.add(fs.getFileStatus(hadoopPath));
        }
        log.trace("resolveFiles() -> {} file(s)", result.size());
        return result;
    }

    private void collectBamChildren(FileSystem fs, Path dir, List<FileStatus> result) throws IOException {
        log.trace("collectBamChildren(dir={})", dir);
        for (FileStatus s : fs.listStatus(dir)) {
            if (!s.isDirectory() && isAcceptedExtension(s.getPath())) {
                log.trace("collectBamChildren() found {}", s.getPath());
                result.add(s);
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
