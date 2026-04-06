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
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
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

    public static final String SAMPLE_NAME = "sample1";

    public record Fixtures(
        java.net.URI plainVcf,
        java.net.URI bgzVcf,
        java.net.URI tbiIndex
    ) {}

    public static Fixtures generate(Path tempDir) throws IOException {
        Path plainPath = tempDir.resolve("test.vcf");
        Path bgzPath   = tempDir.resolve("test.vcf.gz");
        Path tbiPath   = tempDir.resolve("test.vcf.gz.tbi");

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

        return new Fixtures(plainPath.toUri(), bgzPath.toUri(), tbiPath.toUri());
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
        return new VariantContextBuilder()
                .chr(chrom)
                .start(pos)
                .stop(pos)
                .alleles(Arrays.asList(ref, alt))
                .id(id)
                .attribute("DP", dp)
                .attribute("AF", af)
                .genotypes(new GenotypeBuilder(SAMPLE_NAME)
                        .alleles(Arrays.asList(ref, alt))
                        .attribute("DP", dp)
                        .make())
                .make();
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
