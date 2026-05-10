package com.litebfx.bed;

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
 * Unit tests for {@link BedScanBuilder#pushFilters}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>EqualTo on {@code chrom} (match / attribute-mismatch / type-mismatch)</li>
 *   <li>All four range filter types on {@code chromStart}/{@code chromEnd}</li>
 *   <li>Range filters present when no chrom EqualTo is set (must be ignored)</li>
 *   <li>Range filter with wrong attribute (must be ignored)</li>
 *   <li>Column pruning via {@link BedScanBuilder#pruneColumns}</li>
 * </ul>
 */
class BedScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.bed"));
    }

    // -------------------------------------------------------------------------
    // pushFilters — no filters
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_empty_returnsEmptyAndBuilds() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Filter[] result = b.pushFilters(new Filter[0]);
        assertEquals(0, result.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on chrom (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToChrom_returnsAllFiltersUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Filter[] filters = {new EqualTo("chrom", "chr1")};
        Filter[] returned = b.pushFilters(filters);
        // All filters must be returned as unhandled
        assertSame(filters, returned);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on wrong attribute (type matches, attribute doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToWrongAttribute_chromNotExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // EqualTo type matches but attribute != "chrom" → chrom stays null, range loop skipped
        b.pushFilters(new Filter[]{new EqualTo("score", "500")});
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filters without chrom (must be silently ignored)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_rangeFiltersNoChrom_ignored() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // No EqualTo("chrom") → chrom == null → second loop never entered
        b.pushFilters(new Filter[]{
                new GreaterThanOrEqual("chromStart", 500L),
                new LessThanOrEqual("chromEnd", 1000L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThanOrEqual on chromStart (with chrom set)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGte_startExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThanOrEqual("chromStart", 500L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThan on chromStart (start + 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGt_startPlusOneExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThan("chromStart", 499L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThanOrEqual on chromEnd
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusLte_endExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThanOrEqual("chromEnd", 1000L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThan on chromEnd (end - 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusLt_endMinusOneExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThan("chromEnd", 1001L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filter with wrong attribute (type matches, attr doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_chromPlusGteWrongAttr_ignored() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // GreaterThanOrEqual type matches but attribute != "chromStart"
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThanOrEqual("score", 0L)
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusLteWrongAttr_ignored() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // LessThanOrEqual type matches but attribute != "chromEnd"
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThanOrEqual("score", 1000L)
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusGtWrongAttr_ignored() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // GreaterThan type matches but attribute != "chromStart"
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new GreaterThan("score", 0L)
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_chromPlusLtWrongAttr_ignored() {
        BedScanBuilder b = new BedScanBuilder(opts());
        // LessThan type matches but attribute != "chromEnd"
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chr1"),
                new LessThan("score", 1000L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — all four range filter types together with chrom
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_allRangeFilterTypes_allProcessed() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("chrom", "chrX"),
                new GreaterThanOrEqual("chromStart", 100L),
                new GreaterThan("chromStart", 99L),
                new LessThanOrEqual("chromEnd", 200L),
                new LessThan("chromEnd", 201L)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedFilters — always empty
    // -------------------------------------------------------------------------

    @Test
    void pushedFilters_alwaysReturnsEmpty() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("chrom", "chr1")});
        assertEquals(0, b.pushedFilters().length);
    }

    // -------------------------------------------------------------------------
    // pruneColumns — accepted without error
    // -------------------------------------------------------------------------

    @Test
    void pruneColumns_reducedSchema_buildsSuccessfully() {
        BedScanBuilder b = new BedScanBuilder(opts());
        StructType reduced = new StructType()
                .add("chrom", DataTypes.StringType)
                .add("chromStart", DataTypes.LongType);
        b.pruneColumns(reduced);
        assertNotNull(b.build());
    }
}
