package com.litebfx.vcf;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Generates deterministic VCF test fixtures for use in {@link VcfDataSourceTest}.
 *
 * <p>Produces:
 * <ul>
 *   <li>A plain (uncompressed) VCF file ({@code test.vcf}) — 5 variants, no index.</li>
 *   <li>A bgzip-compressed VCF ({@code test.vcf.gz}) with a tabix index ({@code test.vcf.gz.tbi}),
 *       same 5 variants: chr1 (3 records at pos 100, 500, 1000) and chr2 (2 records at pos 200, 800).</li>
 * </ul>
 *
 * <p>All variants are SNPs in a single-sample VCF ({@code sample1}).
 */
public class VcfTestGenerator {

    /** Total variants across both chromosomes. */
    public static final int VCF_TOTAL         = 5;
    /** Variants on chr1. */
    public static final int VCF_CHR1_COUNT    = 3;
    /** Variants on chr2. */
    public static final int VCF_CHR2_COUNT    = 2;
    /** Variants on chr1 with pos >= 500. */
    public static final int VCF_CHR1_FROM_500 = 2;
    /** Number of distinct chromosomes in the test VCF (chr1 + chr2). */
    public static final int VCF_CHROM_COUNT   = 2;

    /** Total variants in the edge-case fixture (PASS filter + multi-filter). */
    public static final int EDGE_TOTAL = 2;

    public static final String SAMPLE_NAME = "sample1";

    public record Fixtures(
        java.net.URI plainVcf,
        java.net.URI bgzVcf,
        java.net.URI tbiIndex,
        java.net.URI bcf
    ) {}

    public static Fixtures generate(Path tempDir) throws IOException {
        Path plainPath = tempDir.resolve("test.vcf");
        Path bgzPath   = tempDir.resolve("test.vcf.gz");
        Path tbiPath   = tempDir.resolve("test.vcf.gz.tbi");
        Path bcfPath   = tempDir.resolve("test.bcf");

        SAMSequenceDictionary dict = new SAMSequenceDictionary(Arrays.asList(
                new SAMSequenceRecord("chr1", 248956422),
                new SAMSequenceRecord("chr2", 242193529)
        ));

        VCFHeader header = buildHeader(dict);
        List<VariantContext> variants = buildVariants();

        // --- Plain VCF ---
        writeVcf(plainPath.toFile(), header, dict, variants,
                VariantContextWriterBuilder.OutputType.VCF);

        // --- Bgzipped VCF ---
        writeVcf(bgzPath.toFile(), header, dict, variants,
                VariantContextWriterBuilder.OutputType.BLOCK_COMPRESSED_VCF);

        // --- Tabix index ---
        TabixIndex tbi = IndexFactory.createTabixIndex(bgzPath.toFile(), new VCFCodec(), null);
        tbi.write(tbiPath.toFile());

        // --- BCF ---
        writeVcf(bcfPath.toFile(), header, dict, variants,
                VariantContextWriterBuilder.OutputType.BCF);

        return new Fixtures(plainPath.toUri(), bgzPath.toUri(), tbiPath.toUri(), bcfPath.toUri());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static VCFHeader buildHeader(SAMSequenceDictionary dict) {
        Set<htsjdk.variant.vcf.VCFHeaderLine> lines = new HashSet<>();
        lines.add(new VCFInfoHeaderLine("DP", 1, VCFHeaderLineType.Integer, "Total read depth"));
        lines.add(new VCFInfoHeaderLine("AF", 1, VCFHeaderLineType.Float,   "Allele frequency"));
        lines.add(new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String,  "Genotype"));
        lines.add(new VCFFormatHeaderLine("DP", 1, VCFHeaderLineType.Integer, "Sample read depth"));
        VCFHeader header = new VCFHeader(lines, List.of(SAMPLE_NAME));
        header.setSequenceDictionary(dict);
        return header;
    }

    private static List<VariantContext> buildVariants() {
        Allele refA = Allele.create("A", true);
        Allele altT = Allele.create("T", false);
        Allele refC = Allele.create("C", true);
        Allele altG = Allele.create("G", false);

        return List.of(
            snp("chr1", 100, "rs1", refA, altT, 30, 0.5),
            snp("chr1", 500, "rs2", refC, altG, 20, 0.3),
            snp("chr1", 1000, "rs3", refA, altT, 25, 0.4),
            snp("chr2", 200, "rs4", refC, altG, 15, 0.6),
            snp("chr2", 800, "rs5", refA, altT, 40, 0.2)
        );
    }

    private static VariantContext snp(String chrom, int pos, String id,
                                      Allele ref, Allele alt,
                                      int dp, double af) {
        // QUAL = dp means log10PError = -dp/10 (Phred scale: QUAL = -10 * log10(P_error))
        return new VariantContextBuilder()
                .chr(chrom)
                .start(pos)
                .stop(pos)
                .alleles(Arrays.asList(ref, alt))
                .id(id)
                .log10PError(-dp / 10.0)
                .attribute("DP", dp)
                .attribute("AF", af)
                .genotypes(new GenotypeBuilder(SAMPLE_NAME)
                        .alleles(Arrays.asList(ref, alt))
                        .attribute("DP", dp)
                        .make())
                .make();
    }

    // -------------------------------------------------------------------------
    // Edge-case fixture (VCFFileReader path: null alt, null qual, Boolean/List INFO, filters)
    // -------------------------------------------------------------------------

    /** Bgzipped VCF covering edge cases in {@code getFromVariantContext}. */
    public record EdgeCaseFixtures(java.net.URI bgzVcf) {}

    /**
     * Generates a bgzipped VCF with two variants that exercise VCFFileReader edge cases:
     * <ul>
     *   <li>Variant 1: PASS filter, flag INFO field (SOMATIC=Boolean.TRUE), multi-value INFO
     *       field (DP4=List[1,2,3,4]).</li>
     *   <li>Variant 2: ref-only site (no alt alleles → alt is null), no QUAL (→ qual is null),
     *       and a non-empty filter string (LowQual).</li>
     * </ul>
     */
    public static EdgeCaseFixtures generateEdgeCases(Path tempDir) throws IOException {
        Path bgzPath = tempDir.resolve("edge_cases.vcf.gz");

        SAMSequenceDictionary dict = new SAMSequenceDictionary(
                Arrays.asList(new SAMSequenceRecord("chr1", 248956422)));

        Set<htsjdk.variant.vcf.VCFHeaderLine> lines = new HashSet<>();
        lines.add(new VCFInfoHeaderLine("DP",     1,                       VCFHeaderLineType.Integer, "Depth"));
        lines.add(new VCFInfoHeaderLine("SOMATIC", 0,                      VCFHeaderLineType.Flag,    "Somatic variant"));
        lines.add(new VCFInfoHeaderLine("DP4",    VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.Integer, "DP4 counts"));
        lines.add(new VCFFormatHeaderLine("GT",   1,                       VCFHeaderLineType.String,  "Genotype"));
        lines.add(new VCFFilterHeaderLine("LowQual", "Low quality variant"));
        VCFHeader header = new VCFHeader(lines, List.of(SAMPLE_NAME));
        header.setSequenceDictionary(dict);

        Allele refA = Allele.create("A", true);
        Allele altT = Allele.create("T", false);

        // Variant 1: PASS filter, Boolean INFO (SOMATIC), List INFO (DP4)
        VariantContext vc1 = new VariantContextBuilder()
                .chr("chr1").start(100).stop(100)
                .alleles(Arrays.asList(refA, altT))
                .id("rs1")
                .log10PError(-3.0)
                .passFilters()
                .attribute("SOMATIC", Boolean.TRUE)
                .attribute("DP4", Arrays.asList(1, 2, 3, 4))
                .genotypes(new GenotypeBuilder(SAMPLE_NAME)
                        .alleles(Arrays.asList(refA, altT)).make())
                .make();

        // Variant 2: ref-only (no alt allele → alt = null), no qual, non-empty filter
        VariantContext vc2 = new VariantContextBuilder()
                .chr("chr1").start(200).stop(200)
                .alleles(Arrays.asList(refA))
                .id(".")
                .filters(new HashSet<>(List.of("LowQual")))
                .attribute("DP", 5)
                .genotypes(new GenotypeBuilder(SAMPLE_NAME)
                        .alleles(Arrays.asList(refA, refA)).make())
                .make();

        writeVcf(bgzPath.toFile(), header, dict, Arrays.asList(vc1, vc2),
                VariantContextWriterBuilder.OutputType.BLOCK_COMPRESSED_VCF);

        return new EdgeCaseFixtures(bgzPath.toUri());
    }

    // -------------------------------------------------------------------------
    // Triple-chromosome fixture (exercises groupChroms extra > 0 path)
    // -------------------------------------------------------------------------

    /** Three-chromosome bgzipped VCF — one SNP per chrom, 3 total. */
    public record TripleChromFixtures(java.net.URI bgzVcf) {}

    /**
     * Generates a three-chromosome bgzipped VCF with one variant per chromosome.
     * Used in tests that need {@code numChroms % numGroups != 0} to exercise the
     * {@code extra > 0} branch in {@code VcfScan.groupChroms()}.
     */
    public static TripleChromFixtures generateTripleChrom(Path tempDir) throws IOException {
        Path bgzPath = tempDir.resolve("triple_chrom.vcf.gz");
        Path tbiPath = tempDir.resolve("triple_chrom.vcf.gz.tbi");

        SAMSequenceDictionary dict = new SAMSequenceDictionary(Arrays.asList(
                new SAMSequenceRecord("chr1", 248956422),
                new SAMSequenceRecord("chr2", 242193529),
                new SAMSequenceRecord("chr3", 198295559)
        ));

        VCFHeader header = buildHeader(dict);

        Allele refA = Allele.create("A", true);
        Allele altT = Allele.create("T", false);
        List<VariantContext> variants = Arrays.asList(
                snp("chr1", 100, "rs1", refA, altT, 30, 0.5),
                snp("chr2", 100, "rs2", refA, altT, 25, 0.4),
                snp("chr3", 100, "rs3", refA, altT, 20, 0.3)
        );

        writeVcf(bgzPath.toFile(), header, dict, variants,
                VariantContextWriterBuilder.OutputType.BLOCK_COMPRESSED_VCF);

        TabixIndex tbi = IndexFactory.createTabixIndex(bgzPath.toFile(), new VCFCodec(), null);
        tbi.write(tbiPath.toFile());

        return new TripleChromFixtures(bgzPath.toUri());
    }

    private static void writeVcf(File outFile, VCFHeader header,
                                 SAMSequenceDictionary dict,
                                 List<VariantContext> variants,
                                 VariantContextWriterBuilder.OutputType type) {
        VariantContextWriter writer = new VariantContextWriterBuilder()
                .setOutputFile(outFile)
                .setOutputFileType(type)
                .setReferenceDictionary(dict)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();
        writer.writeHeader(header);
        for (VariantContext vc : variants) {
            writer.add(vc);
        }
        writer.close();
    }
}
