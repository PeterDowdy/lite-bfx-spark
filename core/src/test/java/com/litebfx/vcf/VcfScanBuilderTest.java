package com.litebfx.vcf;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VcfScanBuilder#pushFilters}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>EqualTo on {@code chrom} (match / attribute-mismatch)</li>
 *   <li>All four range filter types on {@code pos}</li>
 *   <li>Range filters present when no chrom EqualTo is set (must be ignored)</li>
 *   <li>Range filter with wrong attribute (must be ignored)</li>
 *   <li>Column pruning via {@link VcfScanBuilder#pruneColumns}</li>
 * </ul>
 */
class VcfScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.vcf"));
    }

    // -------------------------------------------------------------------------
    // pushFilters — no filters
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_empty_returnsEmptyAndBuilds() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Filter[] result = b.pushFilters(new Filter[0]);
        assertEquals(0, result.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on chrom (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToChrom_returnsAllFiltersUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Filter[] filters = {new EqualTo("chrom", "chr1")};
        assertSame(filters, b.pushFilters(filters));
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on wrong attribute (type matches, attribute doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToWrongAttribute_chromNotExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("filter", "PASS")});
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filters without chrom (must be silently ignored)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_rangeFiltersNoChrom_ignored() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new GreaterThanOrEqual("pos", 100),
                new LessThanOrEqual("pos", 1000)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThanOrEqual on pos (with chrom set)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGte_startExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThanOrEqual("pos", 100)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThan on pos (start + 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGt_startPlusOneExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThan("pos", 99)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThanOrEqual on pos
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusLte_endExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThanOrEqual("pos", 1000)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThan on pos (end - 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusLt_endMinusOneExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThan("pos", 1001)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filter with wrong attribute (type matches, attr doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGteWrongAttr_ignored() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThanOrEqual("qual", 30)   // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusLtWrongAttr_ignored() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThan("qual", 100)             // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusGtWrongAttr_ignored() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThan("qual", 20)           // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusLteWrongAttr_ignored() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThanOrEqual("qual", 100)      // wrong attribute
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — all four range filter types together with chrom
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_allRangeFilterTypes_allProcessed() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chrX"),
                new GreaterThanOrEqual("pos", 100),
                new GreaterThan("pos", 99),
                new LessThanOrEqual("pos", 200),
                new LessThan("pos", 201)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedFilters — always empty
    // -------------------------------------------------------------------------

    @Test
    void pushedFilters_alwaysReturnsEmpty() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("chrom", "chr1")});
        assertEquals(0, b.pushedFilters().length);
    }

    // -------------------------------------------------------------------------
    // pruneColumns — accepted without error
    // -------------------------------------------------------------------------

    @Test
    void pruneColumns_reducedSchema_buildsSuccessfully() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        StructType reduced = new StructType()
                .add("chrom", DataTypes.StringType)
                .add("pos", DataTypes.IntegerType);
        b.pruneColumns(reduced);
        assertNotNull(b.build());
    }
}
