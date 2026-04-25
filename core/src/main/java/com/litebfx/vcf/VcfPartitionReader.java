package com.litebfx.vcf;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
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
 *   <li><b>Multi-chrom tabix path</b> ({@code queryChroms} is non-null): opens a
 *       {@link VCFFileReader} once and chains one tabix region query per chromosome.
 *       Used for per-chromosome parallel partitions planned by
 *       {@code VcfScan.planTabixChromPartitions}.</li>
 *   <li><b>Single-chrom or full-file VCFFileReader path</b> (tabix region query,
 *       bgzipped full-file, or BCF): uses {@link VCFFileReader} auto-detected by
 *       file magic bytes.  When {@code queryChrom} is non-null and a tabix index is
 *       present, a region query is issued; otherwise the full file is iterated.</li>
 *   <li><b>Line-split path</b> (plain-text VCF with byte-range partitions):
 *       uses {@link FSDataInputStream} directly.  The {@code #CHROM} header line is
 *       read from offset 0 to extract sample names; then the reader seeks to
 *       {@code startByte}, discards bytes up to the next newline, and parses
 *       tab-delimited VCF data lines until {@code endByte} is reached.</li>
 * </ul>
 */
public class VcfPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(VcfPartitionReader.class);

    private final VcfInputPartition partition;
    private boolean opened = false;

    // --- VCFFileReader path (tabix region query, bgzipped full-file, or BCF) ---
    private VCFFileReader reader;
    private CloseableIterator<VariantContext> iter;
    private VariantContext current;

    // --- Multi-chrom tabix path ---
    /** Remaining chromosomes to query; non-null in multi-chrom mode. Iterators are created lazily. */
    private Deque<String> pendingChroms;

    // --- Line-split path (plain-text VCF with byte-range partitions) ---
    private FSDataInputStream fsIn;
    private boolean isVcfSplitMode = false;
    private long vcfEndByte = Long.MAX_VALUE;
    private String[] sampleNames = new String[0];
    private String[] currentColumns = null;

    public VcfPartitionReader(VcfInputPartition partition) {
        log.trace("VcfPartitionReader(path={}, queryChrom={})",
                partition.getPath(), partition.getQueryChrom());
        this.partition = partition;
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
        if (isVcfSplitMode) {
            return nextSplitMode();
        }
        if (pendingChroms != null) {
            return nextMultiChromMode();
        }
        if (!iter.hasNext()) return false;
        current = iter.next();
        return true;
    }

    @Override
    public InternalRow get() {
        if (isVcfSplitMode) {
            return getSplitMode();
        }
        return getFromVariantContext(current);
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
                if (fsIn != null) fsIn.close();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Open helpers
    // -------------------------------------------------------------------------

    private void open() {
        long startByte = partition.getStartByte();
        long endByte   = partition.getEndByte();

        // Enter split mode when the partition has a byte-range boundary.
        if (startByte > 0 || endByte != Long.MAX_VALUE) {
            openSplitMode();
            return;
        }

        // Multi-chrom tabix mode: one query per chromosome in the group.
        if (partition.getQueryChroms() != null) {
            openMultiChromMode();
            return;
        }

        // VCFFileReader path: tabix region query, bgzipped full-file, or BCF.
        String pathStr  = partition.getPath();
        String indexStr = partition.getIndexPath();
        String chrom    = partition.getQueryChrom();
        log.trace("open() path={} indexPath={} queryChrom={}", pathStr, indexStr, chrom);

        java.nio.file.Path nioPath = toNioPath(pathStr);

        if (indexStr != null && chrom != null) {
            try {
                java.nio.file.Path nioIndex = toNioPath(indexStr);
                reader = new VCFFileReader(nioPath, nioIndex, true);
                iter = reader.query(chrom, partition.getQueryStart(), partition.getQueryEnd());
                log.trace("open() region query chrom={} start={} end={}",
                        chrom, partition.getQueryStart(), partition.getQueryEnd());
            } catch (Exception e) {
                // TabixIndex(File) does not support remote paths (S3A, HDFS, etc.).
                // Fall back to a full-file scan and let the reader filter in-process.
                log.warn("Could not open index from {}, falling back to full scan: {}", indexStr, e.getMessage());
                if (reader != null) {
                    try { reader.close(); } catch (Exception ex) { log.debug("suppressed exception closing reader", ex); }
                    reader = null;
                }
                reader = new VCFFileReader(nioPath, false);
                iter = reader.iterator();
            }
        } else {
            reader = new VCFFileReader(nioPath, false);
            iter = reader.iterator();
            log.trace("open() full-file scan");
        }
    }

    /**
     * Opens one {@link VCFFileReader} and queues all chromosomes from
     * {@code partition.getQueryChroms()} into {@link #pendingChroms} for lazy iteration.
     * {@link #nextMultiChromMode()} creates one tabix query iterator per chromosome on demand.
     *
     * <p>Falls back to a full-file scan if the tabix index cannot be opened (e.g. remote
     * path that htsjdk's nio provider does not support) — same fallback as the single-chrom
     * path.
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
                // Store chroms for lazy iteration — don't create all query iterators upfront,
                // because each reader.query() seeks the shared underlying stream and invalidates
                // any previously-open iterator.
                pendingChroms = new ArrayDeque<>(Arrays.asList(chroms));
                log.trace("openMultiChromMode() queued {} chroms for lazy iteration", chroms.length);
                return;
            } catch (Exception e) {
                // Index not accessible as a nio path — fall back to full-file scan.
                log.warn("Could not open index from {}, falling back to full scan: {}", indexStr, e.getMessage());
                if (reader != null) {
                    try { reader.close(); } catch (Exception ex) {
                        log.debug("suppressed exception closing reader", ex);
                    }
                    reader = null;
                }
            }
        }

        // Fallback: full-file scan (pendingChroms stays null; iter is used instead).
        reader = new VCFFileReader(nioPath, false);
        iter   = reader.iterator();
        log.trace("openMultiChromMode() fallback to full-file scan");
    }

    /**
     * Lazily creates and drains one tabix query iterator per chromosome from
     * {@link #pendingChroms}.  Sets {@link #current} to the next record and returns
     * {@code true}, or returns {@code false} when all chromosomes are exhausted.
     *
     * <p>Only one query iterator is open at a time; it is created when the previous
     * one is exhausted and closed before the next chrom is queried.  This avoids
     * seeking conflicts: each {@code reader.query()} call repositions the underlying
     * stream and would invalidate a simultaneously-open iterator.
     */
    private boolean nextMultiChromMode() {
        while (true) {
            // Drain the current chromosome's iterator.
            if (iter != null && iter.hasNext()) {
                current = iter.next();
                return true;
            }
            // Current iterator exhausted — close it before opening the next chrom.
            if (iter != null) {
                try { iter.close(); } catch (Exception e) {
                    log.debug("suppressed exception closing exhausted chrom iterator", e);
                }
                iter = null;
            }
            // Advance to the next queued chromosome.
            if (pendingChroms.isEmpty()) return false;
            String chrom = pendingChroms.poll();
            iter = reader.query(chrom, 1, Integer.MAX_VALUE);
        }
    }

    private void openSplitMode() {
        String pathStr  = partition.getPath();
        long startByte  = partition.getStartByte();
        long endByte    = partition.getEndByte();
        log.trace("openSplitMode() path={} startByte={} endByte={}", pathStr, startByte, endByte);

        isVcfSplitMode = true;
        vcfEndByte = endByte;

        try {
            Configuration conf = partition.getHadoopConf();
            Path hadoopPath = new Path(pathStr);
            FileSystem fs = hadoopPath.getFileSystem(conf);
            long fileLength = fs.getFileStatus(hadoopPath).getLen();

            if (startByte > 0 && startByte >= fileLength) {
                // Chunk starts past EOF — empty partition.
                return;
            }

            fsIn = fs.open(hadoopPath);

            // Scan the header from offset 0 to find the #CHROM line and extract sample names.
            String line;
            while ((line = readLineFromStream(fsIn)) != null) {
                if (line.startsWith("#CHROM") || line.startsWith("#chrom")) {
                    sampleNames = parseSampleNames(line);
                    break;
                }
                if (!line.startsWith("#")) {
                    // Hit a data line before finding #CHROM (non-standard VCF) — stop scanning.
                    break;
                }
            }
            // After the loop, fsIn is positioned right after the #CHROM line (= start of data).

            // For startByte > 0, seek to the chunk boundary and skip the partial line.
            if (startByte > 0) {
                fsIn.seek(startByte - 1);
                int prev = fsIn.read(); // advances position to startByte
                if (prev != '\n') {
                    int b;
                    while ((b = fsIn.read()) != -1 && b != '\n') { /* skip partial line */ }
                }
            }
            // For startByte == 0: already positioned right after #CHROM, which is correct.

        } catch (IOException e) {
            throw new RuntimeException("Failed to open VCF split partition for path: " + pathStr, e);
        }
    }

    // -------------------------------------------------------------------------
    // Split-mode next / get
    // -------------------------------------------------------------------------

    private boolean nextSplitMode() throws IOException {
        if (fsIn == null) return false; // empty partition (startByte past EOF)
        while (true) {
            if (vcfEndByte != Long.MAX_VALUE && fsIn.getPos() >= vcfEndByte) return false;
            String line = readLineFromStream(fsIn);
            if (line == null) return false;
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] cols = line.split("\t", -1);
            if (cols.length < 8) continue; // malformed line
            currentColumns = cols;
            return true;
        }
    }

    private InternalRow getSplitMode() {
        String[] c = currentColumns;

        // chrom (col 0)
        UTF8String chrom = UTF8String.fromString(c[0]);

        // pos (col 1, 1-based)
        int pos = Integer.parseInt(c[1].trim());

        // id (col 2) — null when "."
        String idStr = c[2].trim();
        UTF8String id = (".".equals(idStr) || idStr.isEmpty()) ? null : UTF8String.fromString(idStr);

        // ref (col 3)
        UTF8String ref = UTF8String.fromString(c[3]);

        // alt (col 4) — null when "."
        String altStr = c[4].trim();
        UTF8String alt = (".".equals(altStr) || altStr.isEmpty()) ? null : UTF8String.fromString(altStr);

        // qual (col 5) — null when "."
        Double qual = null;
        String qualStr = c[5].trim();
        if (!".".equals(qualStr) && !qualStr.isEmpty()) {
            try { qual = Double.parseDouble(qualStr); } catch (NumberFormatException ignored) { /* null */ }
        }

        // filter (col 6) — null when "." (not applied)
        UTF8String filter = null;
        String filterStr = c[6].trim();
        if (!".".equals(filterStr) && !filterStr.isEmpty()) {
            filter = UTF8String.fromString(filterStr);
        }

        // info (col 7)
        ArrayBasedMapData infoMap = parseInfoString(c[7]);

        // format + genotypes (cols 8+) — absent in sites-only VCFs
        UTF8String format = null;
        ArrayBasedMapData genotypesMap = null;
        if (c.length > 8 && !c[8].trim().isEmpty()) {
            format = UTF8String.fromString(c[8].trim());
            if (sampleNames.length > 0 && c.length > 9) {
                genotypesMap = buildGenotypesFromColumns(c);
            }
        }

        Object[] values = new Object[10];
        values[0] = chrom;
        values[1] = pos;
        values[2] = id;
        values[3] = ref;
        values[4] = alt;
        values[5] = qual;
        values[6] = filter;
        values[7] = infoMap;
        values[8] = format;
        values[9] = genotypesMap;
        return new GenericInternalRow(values);
    }

    // -------------------------------------------------------------------------
    // VCFFileReader-path get
    // -------------------------------------------------------------------------

    private static InternalRow getFromVariantContext(VariantContext vc) {
        // chrom
        UTF8String chrom = UTF8String.fromString(vc.getContig());

        // pos (htsjdk is 1-based, same as VCF)
        int pos = vc.getStart();

        // id — null when "." or absent
        String idStr = vc.getID();
        UTF8String id = (idStr == null || ".".equals(idStr) || idStr.isEmpty())
                ? null : UTF8String.fromString(idStr);

        // ref
        UTF8String ref = UTF8String.fromString(vc.getReference().getDisplayString());

        // alt — comma-joined; null when no alternate alleles
        List<Allele> altAlleles = vc.getAlternateAlleles();
        UTF8String alt = null;
        if (!altAlleles.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < altAlleles.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(altAlleles.get(i).getDisplayString());
            }
            alt = UTF8String.fromString(sb.toString());
        }

        // qual — null when missing
        Double qual = vc.hasLog10PError() ? vc.getPhredScaledQual() : null;

        // filter — null when "." (not applied); "PASS" or semicolon-joined filters otherwise
        UTF8String filter = null;
        if (vc.filtersWereApplied()) {
            if (vc.getFilters().isEmpty()) {
                filter = UTF8String.fromString("PASS");
            } else {
                filter = UTF8String.fromString(String.join(";", vc.getFilters()));
            }
        }

        // info map
        ArrayBasedMapData infoMap = buildInfoMap(vc.getAttributes());

        // format + genotypes — null when no samples
        UTF8String format = null;
        ArrayBasedMapData genotypesMap = null;
        if (!vc.getGenotypes().isEmpty()) {
            Genotype first = vc.getGenotype(0);
            format = UTF8String.fromString(buildFormatString(first));
            genotypesMap = buildGenotypesMap(vc, first);
        }

        Object[] values = new Object[10];
        values[0] = chrom;
        values[1] = pos;
        values[2] = id;
        values[3] = ref;
        values[4] = alt;
        values[5] = qual;
        values[6] = filter;
        values[7] = infoMap;
        values[8] = format;
        values[9] = genotypesMap;
        return new GenericInternalRow(values);
    }

    // -------------------------------------------------------------------------
    // Split-mode helpers
    // -------------------------------------------------------------------------

    /**
     * Parses a raw VCF INFO column string (e.g. {@code "DP=30;AF=0.5"}) into a
     * Spark map of string keys to string values.  Flag fields (no {@code =}) are
     * mapped to {@code "true"}.
     */
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
                vals.add(UTF8String.fromString("true")); // flag field
            }
        }
        return new ArrayBasedMapData(
                new GenericArrayData(keys.toArray()),
                new GenericArrayData(vals.toArray()));
    }

    /**
     * Builds the genotypes map (sample name → FORMAT values) from the raw VCF columns.
     * Sample names are taken from {@link #sampleNames} (parsed from the {@code #CHROM} line).
     * Returns null if there are no samples or no genotype columns.
     */
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

    /**
     * Extracts sample names from the {@code #CHROM} header line.
     * Sample names occupy columns 9+ (0-indexed).
     */
    private static String[] parseSampleNames(String chromLine) {
        String[] cols = chromLine.split("\t", -1);
        if (cols.length <= 9) return new String[0];
        String[] names = new String[cols.length - 9];
        for (int i = 0; i < names.length; i++) {
            names[i] = cols[9 + i].trim();
        }
        return names;
    }

    /**
     * Reads one text line from {@code stream} byte-by-byte, consuming the terminating
     * {@code \n}.  Returns the line content without the line terminator, or {@code null}
     * at EOF with no bytes read.  Using direct stream reads (rather than BufferedReader)
     * keeps {@code stream.getPos()} accurate so the caller can enforce byte-range boundaries.
     */
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
        for (Genotype g : vc.getGenotypes()) {
            genotypes.add(g);
        }

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

    private static java.nio.file.Path toNioPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            if (uri.getScheme() == null) {
                return Paths.get(pathStr);
            }
            return Paths.get(uri);
        } catch (Exception e) {
            return Paths.get(pathStr);
        }
    }
}
