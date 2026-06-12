package com.litebfx.vcf;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VcfScanBuilder#pushPredicates}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>Equality predicate on {@code chrom} (match / attribute-mismatch)</li>
 *   <li>All four range predicate types on {@code pos}</li>
 *   <li>Range predicates present when no chrom equality is set (must be ignored)</li>
 *   <li>Range predicate with wrong attribute (must be ignored)</li>
 *   <li>{@code And}-wrapped compound predicate unwrapping</li>
 *   <li>Column pruning via {@link VcfScanBuilder#pruneColumns}</li>
 * </ul>
 *
 * <p>Note: range predicates ({@code pos} comparisons) are always returned unhandled so
 * Spark post-filters for exactness — tabix overlap queries may return records that
 * fall partially outside the requested range.  Only {@code chrom} equality is pushed.
 */
class VcfScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.vcf"));
    }

    private static Predicate eq(String col, String val) {
        return new Predicate("=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.StringType)
        });
    }

    private static Predicate gte(String col, int val) {
        return new Predicate(">=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.IntegerType)
        });
    }

    private static Predicate gt(String col, int val) {
        return new Predicate(">", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.IntegerType)
        });
    }

    private static Predicate lte(String col, int val) {
        return new Predicate("<=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.IntegerType)
        });
    }

    private static Predicate lt(String col, int val) {
        return new Predicate("<", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.IntegerType)
        });
    }

    // -------------------------------------------------------------------------
    // pushPredicates — no predicates
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_empty_returnsEmptyAndBuilds() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        assertEquals(0, b.pushPredicates(new Predicate[0]).length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — chrom equality (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToChrom_handledNotReturned() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate chrom = eq("chrom", "chr1");
        assertEquals(0, b.pushPredicates(new Predicate[]{chrom}).length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — chrom equality on wrong attribute
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToWrongAttribute_chromNotExtracted() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("filter", "PASS")});
        assertEquals(1, unhandled.length);
        assertEquals(0, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range without chrom (must be silently ignored / returned)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_rangeFiltersNoChrom_allReturnedUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{gte("pos", 100), lte("pos", 1000)});
        assertEquals(2, unhandled.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range types (with chrom):
    // chrom equality is pushed; pos range is returned unhandled for Spark post-filtering
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromPlusGte_chromPushedRangeUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gte("pos", 100)});
        assertEquals(1, unhandled.length, "pos range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromPlusGt_chromPushedRangeUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gt("pos", 99)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromPlusLte_chromPushedRangeUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), lte("pos", 1000)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromPlusLt_chromPushedRangeUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), lt("pos", 1001)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range with wrong attribute (chrom present, wrong range col)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromPlusGteWrongAttr_rangeUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gte("qual", 30)});
        assertEquals(1, unhandled.length, "wrong-attr range predicate should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom should be handled");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — And-wrapped compound predicate unwrapping
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_andWrapped_chromPushedPosUnhandled() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate chrom = eq("chrom", "chr1");
        Predicate pos   = gte("pos", 1000);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{new And(chrom, pos)});
        assertEquals(1, unhandled.length, "pos range from And should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — all four range types together with chrom
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_allRangeTypes_onlyChromPushed() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        Predicate[] preds = {eq("chrom", "chrX"), gte("pos", 100), gt("pos", 99), lte("pos", 200), lt("pos", 201)};
        assertEquals(4, b.pushPredicates(preds).length, "all four pos range predicates should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedPredicates — reflects handled set (equality only)
    // -------------------------------------------------------------------------

    @Test
    void pushedPredicates_returnsHandledPredicates() {
        VcfScanBuilder b = new VcfScanBuilder(opts());
        b.pushPredicates(new Predicate[]{eq("chrom", "chr1")});
        assertEquals(1, b.pushedPredicates().length);
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
