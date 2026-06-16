package io.github.peterdowdy.litebfx.bed;

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
 * Unit tests for {@link BedScanBuilder#pushPredicates}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>Equality predicate on {@code chrom} (match / attribute-mismatch)</li>
 *   <li>All four range predicate types on {@code chromStart}/{@code chromEnd}</li>
 *   <li>Case-insensitive matching for {@code chromStart}/{@code chromEnd}</li>
 *   <li>Range predicates present when no chrom equality is set (must be returned unhandled)</li>
 *   <li>Range predicate with wrong attribute (must be returned unhandled)</li>
 *   <li>{@code And}-wrapped compound predicate unwrapping</li>
 *   <li>Column pruning via {@link BedScanBuilder#pruneColumns}</li>
 * </ul>
 *
 * <p>Note: range predicates ({@code chromStart}/{@code chromEnd} comparisons) are always
 * returned unhandled so Spark post-filters for exactness — tabix overlap queries may
 * return records that fall partially outside the requested range.  Only {@code chrom}
 * equality is pushed.
 */
class BedScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.bed"));
    }

    private static Predicate eq(String col, String val) {
        return new Predicate("=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.StringType)
        });
    }

    private static Predicate gte(String col, long val) {
        return new Predicate(">=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.LongType)
        });
    }

    private static Predicate gt(String col, long val) {
        return new Predicate(">", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.LongType)
        });
    }

    private static Predicate lte(String col, long val) {
        return new Predicate("<=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.LongType)
        });
    }

    private static Predicate lt(String col, long val) {
        return new Predicate("<", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.LongType)
        });
    }

    // -------------------------------------------------------------------------
    // pushPredicates — no predicates
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_empty_returnsEmptyAndBuilds() {
        BedScanBuilder b = new BedScanBuilder(opts());
        assertEquals(0, b.pushPredicates(new Predicate[0]).length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — chrom equality (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToChrom_handledNotReturned() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate chrom = eq("chrom", "chr1");
        assertEquals(0, b.pushPredicates(new Predicate[]{chrom}).length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — case-insensitive chromStart/chromEnd (Spark 4.x lowercasing)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromStartLowercase_handled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] preds = {eq("chrom", "chr1"), gte("chromstart", 500L)};
        Predicate[] unhandled = b.pushPredicates(preds);
        assertEquals(1, unhandled.length, "chromStart range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromEndLowercase_handled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] preds = {eq("chrom", "chr1"), lte("chromend", 1000L)};
        Predicate[] unhandled = b.pushPredicates(preds);
        assertEquals(1, unhandled.length, "chromEnd range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — chrom equality on wrong attribute
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToWrongAttribute_chromNotExtracted() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("score", "500")});
        assertEquals(1, unhandled.length);
        assertEquals(0, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range without chrom (must be returned unhandled)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_rangeFiltersNoChrom_allReturnedUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{gte("chromStart", 500L), lte("chromEnd", 1000L)});
        assertEquals(2, unhandled.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — gte/gt on chromStart (with chrom):
    // chrom equality is pushed; chromStart range is returned unhandled for Spark post-filtering
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromPlusGte_chromPushedRangeUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gte("chromStart", 500L)});
        assertEquals(1, unhandled.length, "chromStart range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromPlusGt_chromPushedRangeUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gt("chromStart", 499L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — lte/lt on chromEnd (with chrom):
    // chrom equality is pushed; chromEnd range is returned unhandled for Spark post-filtering
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromPlusLte_chromPushedRangeUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), lte("chromEnd", 1000L)});
        assertEquals(1, unhandled.length, "chromEnd range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_chromPlusLt_chromPushedRangeUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), lt("chromEnd", 1001L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range with wrong attribute (chrom present, wrong range col)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_chromPlusGteWrongAttr_rangeUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("chrom", "chr1"), gte("score", 0L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — And-wrapped compound predicate
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_andWrapped_chromPushedStartUnhandled() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate chrom = eq("chrom", "chr1");
        Predicate start = gte("chromStart", 500L);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{new And(chrom, start)});
        assertEquals(1, unhandled.length, "chromStart range from And should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — all four range types together with chrom
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_allRangeTypes_onlyChromPushed() {
        BedScanBuilder b = new BedScanBuilder(opts());
        Predicate[] preds = {
                eq("chrom", "chrX"),
                gte("chromStart", 100L),
                gt("chromStart", 99L),
                lte("chromEnd", 200L),
                lt("chromEnd", 201L)
        };
        assertEquals(4, b.pushPredicates(preds).length, "all range predicates should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only chrom equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedPredicates — reflects handled set (equality only)
    // -------------------------------------------------------------------------

    @Test
    void pushedPredicates_returnsHandledPredicates() {
        BedScanBuilder b = new BedScanBuilder(opts());
        b.pushPredicates(new Predicate[]{eq("chrom", "chr1")});
        assertEquals(1, b.pushedPredicates().length);
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
