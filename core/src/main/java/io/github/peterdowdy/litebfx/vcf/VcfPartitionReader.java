package io.github.peterdowdy.litebfx.vcf;

import io.github.peterdowdy.litebfx.HadoopSeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.Block;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.samtools.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Reads {@link VariantContext} records from a single {@link VcfInputPartition}
 * and converts them to Spark {@link InternalRow}s matching {@link VcfSchema#SCHEMA}.
 *
 * <h3>Reading modes</h3>
 * <ul>
 *   <li><b>Line-split path</b> (plain-text VCF with byte-range partitions):
 *       uses {@link FSDataInputStream} directly — works for local and cloud URIs.
 *       The {@code #CHROM} header line is read from offset 0 to extract sample names;
 *       then the reader seeks to {@code startByte}, discards bytes up to the next
 *       newline, and parses tab-delimited VCF data lines until {@code endByte}.</li>
 *   <li><b>BGZF Hadoop path</b> (bgzipped {@code .vcf.gz} on any URI, including
 *       {@code s3a://}, {@code gs://}, {@code wasb://}): opens the file via Hadoop
 *       {@link FileSystem}, wraps it in {@link BlockCompressedInputStream} via
 *       {@link HadoopSeekableStream}, and reads decompressed VCF lines.  When a tabix
 *       index is available it is loaded via Hadoop FS and the reader seeks to the
 *       relevant BGZF blocks; otherwise the file is read sequentially.  Per-chromosome
 *       and single-region query modes are supported.</li>
 *   <li><b>VCFFileReader path</b> (local files only — plain VCF, bgzipped VCF, BCF):
 *       uses {@link VCFFileReader} backed by Java NIO.  Only active for {@code file://}
 *       or scheme-less paths; cloud URIs use the BGZF Hadoop path instead.</li>
 * </ul>
 *
 * <p>BCF ({@code .bcf}) is not supported for remote URIs — BCF is a binary format
 * that requires htsjdk's binary codec, which cannot read from a Hadoop
 * {@link FSDataInputStream} without VCFFileReader.  Convert BCF to {@code .vcf.gz}
 * for cloud storage.
 */
public class VcfPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(VcfPartitionReader.class);

    private final VcfInputPartition partition;
    private final boolean includeInfo;
    private final boolean includeGenotypes;
    private boolean includeFileMetadata = false;
    private InternalRow fileMetadataRow;
    private boolean opened = false;
    private long rowsRead = 0;

    /** Reusable empty-map sentinel returned when info is not needed (info is non-nullable). */
    private static final ArrayBasedMapData EMPTY_MAP = new ArrayBasedMapData(
            new GenericArrayData(new Object[0]), new GenericArrayData(new Object[0]));

    // --- VCFFileReader path (local only: BCF, bgzipped VCF, plain VCF) ---
    private VCFFileReader reader;
    private CloseableIterator<VariantContext> iter;
    private VariantContext current;

    // --- VCFFileReader multi-chrom tabix path (local) ---
    private Deque<String> pendingChroms;

    // --- Line-split path (plain-text VCF, any URI) ---
    private FSDataInputStream fsIn;
    private boolean isVcfSplitMode = false;
    private long vcfEndByte = Long.MAX_VALUE;

    // --- BGZF Hadoop path (bgzipped VCF, any URI including cloud) ---
    private boolean isVcfBgzfMode = false;
    private BlockCompressedInputStream bcis;
    private FSDataInputStream bgzfFsIn;
    private TabixIndex tabixIdx;
    // Remaining blocks for the current chromosome; null in full-scan mode.
    private Deque<Block> bgzfBlocks;
    // VFO past which we stop reading from the current block.
    private long bgzfBlockEnd = Long.MAX_VALUE;
    // Remaining chromosomes in multi-chrom tabix mode.
    private Deque<String> bgzfPendingChroms;

    // --- Shared split / BGZF state ---
    private String[] sampleNames = new String[0];
    private String[] currentColumns = null;

    VcfPartitionReader(VcfInputPartition partition) {
        this(partition, true, true);
    }

    VcfPartitionReader(VcfInputPartition partition, boolean includeInfo, boolean includeGenotypes) {
        this(partition, includeInfo, includeGenotypes, false);
    }

    VcfPartitionReader(VcfInputPartition partition, boolean includeInfo, boolean includeGenotypes,
                       boolean includeFileMetadata) {
        log.trace("VcfPartitionReader(path={}, queryChrom={}, includeInfo={}, includeGenotypes={}, fileMeta={})",
                partition.getPath(), partition.getQueryChrom(), includeInfo, includeGenotypes, includeFileMetadata);
        this.partition        = partition;
        this.includeInfo      = includeInfo;
        this.includeGenotypes = includeGenotypes;
        this.includeFileMetadata = includeFileMetadata;
    }

    // -------------------------------------------------------------------------
    // PartitionReader contract
    // -------------------------------------------------------------------------

    @Override
    public boolean next() throws IOException {
        if (!opened) {
            open();
            opened = true;
        }
        if (includeFileMetadata && fileMetadataRow == null) {
            // Tabix/CSI index is used only for region queries (single or multi-chrom).
            boolean usedIndex = partition.getQueryChrom() != null || partition.getQueryChroms() != null;
            String idx = usedIndex ? partition.getIndexPath() : null;
            fileMetadataRow = io.github.peterdowdy.litebfx.FileMetadata.row(
                    partition.getHadoopConf(), partition.getPath(), idx);
        }
        if (rowsRead >= partition.getRowLimit()) return false;
        boolean hasNext = nextRecord();
        if (hasNext) rowsRead++;
        return hasNext;
    }

    private boolean nextRecord() throws IOException {
        if (isVcfSplitMode) return nextSplitMode();
        if (isVcfBgzfMode)  return nextBgzfMode();
        if (pendingChroms != null) return nextMultiChromMode();
        if (!iter.hasNext()) return false;
        current = iter.next();
        return true;
    }

    @Override
    public InternalRow get() {
        InternalRow row = (isVcfSplitMode || isVcfBgzfMode) ? getSplitMode() : getFromVariantContext(current);
        return includeFileMetadata
                ? io.github.peterdowdy.litebfx.FileMetadata.appendTo(row, fileMetadataRow) : row;
    }

    @Override
    public void close() throws IOException {
        log.trace("close()");
        try {
            if (iter != null) iter.close();
        } finally {
            try {
                if (reader != null) reader.close();
            } finally {
                try {
                    if (bcis != null) {
                        bcis.close(); // closes bgzfFsIn via HadoopSeekableStream
                        bgzfFsIn = null;
                    }
                } finally {
                    try {
                        if (bgzfFsIn != null) bgzfFsIn.close();
                    } finally {
                        if (fsIn != null) fsIn.close();
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Open helpers
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        long startByte = partition.getStartByte();
        long endByte   = partition.getEndByte();

        // Line-split mode: byte-range partition of a plain-text VCF.
        if (startByte > 0 || endByte != Long.MAX_VALUE) {
            openSplitMode();
            return;
        }

        String pathStr = partition.getPath();

        // Remote path: use Hadoop FS so cloud URIs (s3a://, gs://, wasb://) work.
        if (!isLocalPath(pathStr)) {
            if (isBcf(pathStr)) {
                throw new UnsupportedOperationException(
                    "BCF is not supported for remote paths (" + pathStr + "). " +
                    "Convert to .vcf.gz for cloud storage.");
            }
            openBgzfHadoopMode();
            return;
        }

        // Local path: use VCFFileReader (handles BCF, bgzipped VCF, plain VCF).
        if (partition.getQueryChroms() != null) {
            openMultiChromMode();
            return;
        }

        openLocalVcfFileReader();
    }

    /**
     * Opens a local VCF/BCF/bgzipped-VCF file via {@link VCFFileReader}.
     * Used for single-region tabix queries and full-file scans on local paths.
     * Falls back to a full-file scan if the tabix index cannot be opened.
     */
    private void openLocalVcfFileReader() {
        String pathStr  = partition.getPath();
        String indexStr = partition.getIndexPath();
        String chrom    = partition.getQueryChrom();
        java.nio.file.Path nioPath = toNioPath(pathStr);
        log.trace("openLocalVcfFileReader() path={} indexPath={} queryChrom={}", pathStr, indexStr, chrom);

        if (indexStr != null && chrom != null) {
            try {
                java.nio.file.Path nioIndex = toNioPath(indexStr);
                reader = new VCFFileReader(nioPath, nioIndex, true);
                iter = reader.query(chrom, partition.getQueryStart(), partition.getQueryEnd());
                log.trace("openLocalVcfFileReader() region query chrom={} start={} end={}",
                        chrom, partition.getQueryStart(), partition.getQueryEnd());
                return;
            } catch (Exception e) {
                log.warn("Could not open index from {}, falling back to full scan: {}",
                        indexStr, e.getMessage());
                if (reader != null) {
                    try { reader.close(); } catch (Exception ex) {
                        log.debug("suppressed exception closing reader", ex);
                    }
                    reader = null;
                }
            }
        }

        reader = new VCFFileReader(nioPath, false);
        iter = reader.iterator();
        log.trace("openLocalVcfFileReader() full-file scan");
    }

    /**
     * Opens a bgzipped VCF via Hadoop {@link FileSystem} so cloud URIs work.
     *
     * <p>The VCF header is parsed from the decompressed stream to extract sample names.
     * If a tabix index is available it is loaded via Hadoop FS; the reader then seeks
     * to the relevant BGZF blocks rather than scanning the whole file.
     *
     * <p>Mode selection (checked after loading the index):
     * <ol>
     *   <li>{@code queryChroms} non-null → multi-chrom tabix: one block set per chrom.</li>
     *   <li>{@code queryChrom} non-null and tabix available → single-region tabix query.</li>
     *   <li>No tabix → sequential full-scan of the decompressed stream.</li>
     * </ol>
     */
    private void openBgzfHadoopMode() throws IOException {
        String pathStr = partition.getPath();
        String indexStr = partition.getIndexPath();
        Configuration conf = partition.getHadoopConf();
        log.trace("openBgzfHadoopMode() path={} indexPath={}", pathStr, indexStr);

        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        long fileLen = fs.getFileStatus(hadoopPath).getLen();

        bgzfFsIn = fs.open(hadoopPath);
        bcis = new BlockCompressedInputStream(
                new HadoopSeekableStream(bgzfFsIn, fileLen, pathStr));

        // Parse VCF header from the decompressed stream to get sample names.
        String line;
        while ((line = bcis.readLine()) != null) {
            if (line.startsWith("#CHROM") || line.startsWith("#chrom")) {
                sampleNames = parseSampleNames(line);
                break;
            }
            if (!line.startsWith("#")) break; // non-standard: data before #CHROM
        }
        // bcis is now positioned at (or just past) the first data line.

        // Load tabix index via Hadoop FS if available.
        if (indexStr != null) {
            try {
                Path idxPath = new Path(indexStr);
                FileSystem idxFs = idxPath.getFileSystem(conf);
                try (FSDataInputStream idxIn = idxFs.open(idxPath);
                     BlockCompressedInputStream idxBcis = new BlockCompressedInputStream(idxIn)) {
                    tabixIdx = new TabixIndex(idxBcis);
                    log.trace("openBgzfHadoopMode() loaded tabix index from {}", indexStr);
                }
            } catch (Exception e) {
                log.warn("Could not load tabix index from {}, falling back to full scan: {}",
                        indexStr, e.getMessage());
                tabixIdx = null;
            }
        }

        String[] queryChroms = partition.getQueryChroms();
        String queryChrom = partition.getQueryChrom();

        if (tabixIdx != null && queryChroms != null) {
            // Multi-chrom tabix: queue all chromosomes, seek to first chrom's blocks.
            bgzfPendingChroms = new ArrayDeque<>(Arrays.asList(queryChroms));
            advanceToBgzfChrom();
        } else if (tabixIdx != null && queryChrom != null) {
            // Single-region tabix query.
            List<Block> blocks = tabixIdx.getBlocks(
                    queryChrom, partition.getQueryStart(), partition.getQueryEnd());
            bgzfBlocks = new ArrayDeque<>(blocks);
            log.trace("openBgzfHadoopMode() single-chrom tabix: {} blocks for {}",
                    blocks.size(), queryChrom);
            advanceToBgzfBlock();
        }
        // else: full sequential scan — bcis stays at current position after header.

        isVcfBgzfMode = true;
    }

    /** Advances to the first block of the next chromosome in {@link #bgzfPendingChroms}. */
    private void advanceToBgzfChrom() throws IOException {
        if (bgzfPendingChroms == null || bgzfPendingChroms.isEmpty()) {
            bgzfBlocks = null;
            return;
        }
        String chrom = bgzfPendingChroms.poll();
        List<Block> blocks = tabixIdx.getBlocks(chrom, 1, Integer.MAX_VALUE);
        bgzfBlocks = new ArrayDeque<>(blocks);
        log.trace("advanceToBgzfChrom() chrom={} blocks={}", chrom, blocks.size());
        advanceToBgzfBlock();
    }

    /** Seeks {@link #bcis} to the start of the next pending BGZF block. */
    private void advanceToBgzfBlock() throws IOException {
        bgzfBlockEnd = Long.MAX_VALUE;
        if (bgzfBlocks == null || bgzfBlocks.isEmpty()) return;
        Block block = bgzfBlocks.poll();
        bcis.seek(block.getStartPosition());
        bgzfBlockEnd = block.getEndPosition();
        log.trace("advanceToBgzfBlock() start={} end={}", block.getStartPosition(), bgzfBlockEnd);
    }

    /**
     * Opens one {@link VCFFileReader} for multi-chrom tabix queries against a local file.
     * Falls back to full-file scan if the tabix index cannot be opened.
     */
    private void openMultiChromMode() {
        String   pathStr  = partition.getPath();
        String   indexStr = partition.getIndexPath();
        String[] chroms   = partition.getQueryChroms();
        log.trace("openMultiChromMode() path={} indexPath={} chroms={}", pathStr, indexStr, (Object) chroms);

        java.nio.file.Path nioPath = toNioPath(pathStr);

        if (indexStr != null) {
            try {
                java.nio.file.Path nioIndex = toNioPath(indexStr);
                reader = new VCFFileReader(nioPath, nioIndex, true);
                pendingChroms = new ArrayDeque<>(Arrays.asList(chroms));
                log.trace("openMultiChromMode() queued {} chroms for lazy iteration", chroms.length);
                return;
            } catch (Exception e) {
                log.warn("Could not open index from {}, falling back to full scan: {}",
                        indexStr, e.getMessage());
                if (reader != null) {
                    try { reader.close(); } catch (Exception ex) {
                        log.debug("suppressed exception closing reader", ex);
                    }
                    reader = null;
                }
            }
        }

        reader = new VCFFileReader(nioPath, false);
        iter   = reader.iterator();
        log.trace("openMultiChromMode() fallback to full-file scan");
    }

    private void openSplitMode() throws IOException {
        String pathStr  = partition.getPath();
        long startByte  = partition.getStartByte();
        long endByte    = partition.getEndByte();
        log.trace("openSplitMode() path={} startByte={} endByte={}", pathStr, startByte, endByte);

        isVcfSplitMode = true;
        vcfEndByte = endByte;

        Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(hadoopPath).getLen();

        if (startByte > 0 && startByte >= fileLength) {
            return; // empty partition
        }

        fsIn = fs.open(hadoopPath);

        // Scan the header from offset 0 to find the #CHROM line and extract sample names.
        String line;
        while ((line = readLineFromStream(fsIn)) != null) {
            if (line.startsWith("#CHROM") || line.startsWith("#chrom")) {
                sampleNames = parseSampleNames(line);
                break;
            }
            if (!line.startsWith("#")) break;
        }

        if (startByte > 0) {
            fsIn.seek(startByte - 1);
            int prev = fsIn.read();
            if (prev != '\n') {
                int b;
                while ((b = fsIn.read()) != -1 && b != '\n') { /* skip partial line */ }
            }
        }
    }

    // -------------------------------------------------------------------------
    // next() implementations
    // -------------------------------------------------------------------------

    private boolean nextBgzfMode() throws IOException {
        while (true) {
            // In tabix-guided mode, enforce block boundaries.
            if (bgzfBlocks != null || bgzfPendingChroms != null) {
                if (bgzfBlockEnd != Long.MAX_VALUE && bcis.getPosition() >= bgzfBlockEnd) {
                    // Current block exhausted — advance to next block for this chrom.
                    if (bgzfBlocks != null && !bgzfBlocks.isEmpty()) {
                        advanceToBgzfBlock();
                        continue;
                    }
                    // No more blocks for this chrom — advance to next chrom.
                    if (bgzfPendingChroms != null && !bgzfPendingChroms.isEmpty()) {
                        advanceToBgzfChrom();
                        continue;
                    }
                    return false;
                }
            }

            String line = bcis.readLine();
            if (line == null) {
                // EOF reached — try next chrom if we're in multi-chrom mode.
                if (bgzfPendingChroms != null && !bgzfPendingChroms.isEmpty()) {
                    advanceToBgzfChrom();
                    continue;
                }
                return false;
            }
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] cols = line.split("\t", -1);
            if (cols.length < 8) continue; // malformed
            currentColumns = cols;
            return true;
        }
    }

    private boolean nextSplitMode() throws IOException {
        if (fsIn == null) return false;
        while (true) {
            if (vcfEndByte != Long.MAX_VALUE && fsIn.getPos() >= vcfEndByte) return false;
            String line = readLineFromStream(fsIn);
            if (line == null) return false;
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] cols = line.split("\t", -1);
            if (cols.length < 8) continue;
            currentColumns = cols;
            return true;
        }
    }

    private boolean nextMultiChromMode() {
        while (true) {
            if (iter != null && iter.hasNext()) {
                current = iter.next();
                return true;
            }
            if (iter != null) {
                try { iter.close(); } catch (Exception e) {
                    log.debug("suppressed exception closing exhausted chrom iterator", e);
                }
                iter = null;
            }
            if (pendingChroms.isEmpty()) return false;
            String chrom = pendingChroms.poll();
            iter = reader.query(chrom, 1, Integer.MAX_VALUE);
        }
    }

    // -------------------------------------------------------------------------
    // get() implementations
    // -------------------------------------------------------------------------

    private InternalRow getSplitMode() {
        String[] c = currentColumns;

        UTF8String chrom = UTF8String.fromString(c[0]);
        int pos = Integer.parseInt(c[1].trim());

        String idStr = c[2].trim();
        UTF8String id = (".".equals(idStr) || idStr.isEmpty()) ? null : UTF8String.fromString(idStr);

        UTF8String ref = UTF8String.fromString(c[3]);

        String altStr = c[4].trim();
        GenericArrayData alt = null;
        if (!".".equals(altStr) && !altStr.isEmpty()) {
            String[] alleles = altStr.split(",", -1);
            Object[] altArr = new Object[alleles.length];
            for (int i = 0; i < alleles.length; i++) {
                altArr[i] = UTF8String.fromString(alleles[i]);
            }
            alt = new GenericArrayData(altArr);
        }

        Double qual = null;
        String qualStr = c[5].trim();
        if (!".".equals(qualStr) && !qualStr.isEmpty()) {
            try { qual = Double.parseDouble(qualStr); } catch (NumberFormatException ignored) { }
        }

        UTF8String filter = null;
        String filterStr = c[6].trim();
        if (!".".equals(filterStr) && !filterStr.isEmpty()) {
            filter = UTF8String.fromString(filterStr);
        }

        ArrayBasedMapData infoMap = includeInfo ? parseInfoString(c[7]) : EMPTY_MAP;

        UTF8String format = null;
        ArrayBasedMapData genotypesMap = null;
        if (includeGenotypes && c.length > 8 && !c[8].trim().isEmpty()) {
            format = UTF8String.fromString(c[8].trim());
            if (sampleNames.length > 0 && c.length > 9) {
                genotypesMap = buildGenotypesFromColumns(c);
            }
        }

        Object[] values = new Object[10];
        values[0] = chrom; values[1] = pos;   values[2] = id;     values[3] = ref;
        values[4] = alt;   values[5] = qual;  values[6] = filter; values[7] = infoMap;
        values[8] = format; values[9] = genotypesMap;
        return new GenericInternalRow(values);
    }

    private InternalRow getFromVariantContext(VariantContext vc) {
        UTF8String chrom = UTF8String.fromString(vc.getContig());
        int pos = vc.getStart();

        String idStr = vc.getID();
        UTF8String id = (idStr == null || ".".equals(idStr) || idStr.isEmpty())
                ? null : UTF8String.fromString(idStr);

        UTF8String ref = UTF8String.fromString(vc.getReference().getDisplayString());

        List<Allele> altAlleles = vc.getAlternateAlleles();
        GenericArrayData alt = null;
        if (!altAlleles.isEmpty()) {
            Object[] altArr = new Object[altAlleles.size()];
            for (int i = 0; i < altAlleles.size(); i++) {
                altArr[i] = UTF8String.fromString(altAlleles.get(i).getDisplayString());
            }
            alt = new GenericArrayData(altArr);
        }

        Double qual = vc.hasLog10PError() ? vc.getPhredScaledQual() : null;

        UTF8String filter = null;
        if (vc.filtersWereApplied()) {
            filter = vc.getFilters().isEmpty()
                    ? UTF8String.fromString("PASS")
                    : UTF8String.fromString(String.join(";", vc.getFilters()));
        }

        ArrayBasedMapData infoMap = includeInfo ? buildInfoMap(vc.getAttributes()) : EMPTY_MAP;

        UTF8String format = null;
        ArrayBasedMapData genotypesMap = null;
        if (includeGenotypes && !vc.getGenotypes().isEmpty()) {
            Genotype first = vc.getGenotype(0);
            format = UTF8String.fromString(buildFormatString(first));
            genotypesMap = buildGenotypesMap(vc, first);
        }

        Object[] values = new Object[10];
        values[0] = chrom; values[1] = pos;   values[2] = id;     values[3] = ref;
        values[4] = alt;   values[5] = qual;  values[6] = filter; values[7] = infoMap;
        values[8] = format; values[9] = genotypesMap;
        return new GenericInternalRow(values);
    }

    // -------------------------------------------------------------------------
    // Parsing helpers (shared by split and BGZF modes)
    // -------------------------------------------------------------------------

    private static ArrayBasedMapData parseInfoString(String info) {
        String trimmed = info.trim();
        if (".".equals(trimmed) || trimmed.isEmpty()) {
            return new ArrayBasedMapData(
                    new GenericArrayData(new Object[0]),
                    new GenericArrayData(new Object[0]));
        }
        String[] pairs = trimmed.split(";", -1);
        List<Object> keys = new ArrayList<>(pairs.length);
        List<Object> vals = new ArrayList<>(pairs.length);
        for (String pair : pairs) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            if (eq >= 0) {
                keys.add(UTF8String.fromString(pair.substring(0, eq)));
                vals.add(UTF8String.fromString(pair.substring(eq + 1)));
            } else {
                keys.add(UTF8String.fromString(pair));
                vals.add(UTF8String.fromString("true"));
            }
        }
        return new ArrayBasedMapData(
                new GenericArrayData(keys.toArray()),
                new GenericArrayData(vals.toArray()));
    }

    private ArrayBasedMapData buildGenotypesFromColumns(String[] cols) {
        int n = Math.min(sampleNames.length, cols.length - 9);
        if (n <= 0) return null;
        Object[] keys = new Object[n];
        Object[] vals = new Object[n];
        for (int i = 0; i < n; i++) {
            keys[i] = UTF8String.fromString(sampleNames[i]);
            vals[i] = UTF8String.fromString(cols[9 + i]);
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(vals));
    }

    private static String[] parseSampleNames(String chromLine) {
        String[] cols = chromLine.split("\t", -1);
        if (cols.length <= 9) return new String[0];
        String[] names = new String[cols.length - 9];
        for (int i = 0; i < names.length; i++) names[i] = cols[9 + i].trim();
        return names;
    }

    private static String readLineFromStream(FSDataInputStream stream) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        int b;
        while ((b = stream.read()) != -1) {
            if (b == '\n') return sb.toString();
            if (b != '\r') sb.append((char) b);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    // -------------------------------------------------------------------------
    // VCFFileReader-path helpers
    // -------------------------------------------------------------------------

    private static ArrayBasedMapData buildInfoMap(Map<String, Object> attrs) {
        int n = attrs.size();
        Object[] keys   = new Object[n];
        Object[] values = new Object[n];
        int i = 0;
        for (Map.Entry<String, Object> e : attrs.entrySet()) {
            keys[i]   = UTF8String.fromString(e.getKey());
            values[i] = encodeInfoValue(e.getValue());
            i++;
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
    }

    private static UTF8String encodeInfoValue(Object value) {
        if (value == null) return UTF8String.fromString(".");
        if (value instanceof Boolean) return UTF8String.fromString(Boolean.TRUE.equals(value) ? "true" : "false");
        if (value instanceof List<?> list) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(list.get(i));
            }
            return UTF8String.fromString(sb.toString());
        }
        return UTF8String.fromString(value.toString());
    }

    private static String buildFormatString(Genotype first) {
        List<String> keys = new ArrayList<>();
        keys.add("GT");
        keys.addAll(first.getExtendedAttributes().keySet());
        return String.join(":", keys);
    }

    private static ArrayBasedMapData buildGenotypesMap(VariantContext vc, Genotype first) {
        List<String> extKeys = new ArrayList<>(first.getExtendedAttributes().keySet());
        List<Genotype> genotypes = new ArrayList<>();
        for (Genotype g : vc.getGenotypes()) genotypes.add(g);

        Object[] gtKeys   = new Object[genotypes.size()];
        Object[] gtValues = new Object[genotypes.size()];
        for (int i = 0; i < genotypes.size(); i++) {
            Genotype g = genotypes.get(i);
            List<String> parts = new ArrayList<>();
            parts.add(g.getGenotypeString(false));
            for (String key : extKeys) {
                Object val = g.getExtendedAttribute(key);
                parts.add(val != null ? val.toString() : ".");
            }
            gtKeys[i]   = UTF8String.fromString(g.getSampleName());
            gtValues[i] = UTF8String.fromString(String.join(":", parts));
        }
        return new ArrayBasedMapData(new GenericArrayData(gtKeys), new GenericArrayData(gtValues));
    }

    // -------------------------------------------------------------------------
    // Path helpers
    // -------------------------------------------------------------------------

    /**
     * Returns true when the path refers to the local filesystem (null scheme or "file" scheme).
     * Cloud URIs ({@code s3a://}, {@code gs://}, {@code wasb://}, {@code hdfs://}, etc.)
     * return false and are handled via Hadoop FS.
     */
    static boolean isLocalPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            String scheme = uri.getScheme();
            return scheme == null || scheme.equals("file");
        } catch (Exception e) {
            return true;
        }
    }

    private static boolean isBcf(String pathStr) {
        return pathStr.toLowerCase().endsWith(".bcf");
    }

    private static java.nio.file.Path toNioPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            if (uri.getScheme() == null) return Paths.get(pathStr);
            return Paths.get(uri);
        } catch (Exception e) {
            return Paths.get(pathStr);
        }
    }
}
