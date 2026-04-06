package com.litebfx.vcf;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads {@link VariantContext} records from a single {@link VcfInputPartition}
 * and converts them to Spark {@link InternalRow}s matching {@link VcfSchema#SCHEMA}.
 *
 * <p>Uses {@link VCFFileReader} which auto-detects VCF, bgzipped VCF, and BCF from
 * file magic bytes. When {@code queryChrom} is non-null and a tabix index is present,
 * a region query is issued; otherwise the full file is iterated.
 */
public class VcfPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(VcfPartitionReader.class);

    private final VcfInputPartition partition;

    private VCFFileReader reader;
    private CloseableIterator<VariantContext> iter;
    private VariantContext current;

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
        if (reader == null) {
            open();
        }
        if (!iter.hasNext()) return false;
        current = iter.next();
        return true;
    }

    @Override
    public InternalRow get() {
        VariantContext vc = current;

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

    @Override
    public void close() throws IOException {
        log.trace("close()");
        try {
            if (iter != null) iter.close();
        } finally {
            if (reader != null) reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void open() {
        String pathStr  = partition.getPath();
        String indexStr = partition.getIndexPath();
        String chrom    = partition.getQueryChrom();
        log.trace("open() path={} indexPath={} queryChrom={}", pathStr, indexStr, chrom);

        Path nioPath = toNioPath(pathStr);

        if (indexStr != null && chrom != null) {
            Path nioIndex = toNioPath(indexStr);
            reader = new VCFFileReader(nioPath, nioIndex, true);
            iter = reader.query(chrom, partition.getQueryStart(), partition.getQueryEnd());
            log.trace("open() region query chrom={} start={} end={}",
                    chrom, partition.getQueryStart(), partition.getQueryEnd());
        } else {
            reader = new VCFFileReader(nioPath, false);
            iter = reader.iterator();
            log.trace("open() full-file scan");
        }
    }

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

    private static Path toNioPath(String pathStr) {
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
