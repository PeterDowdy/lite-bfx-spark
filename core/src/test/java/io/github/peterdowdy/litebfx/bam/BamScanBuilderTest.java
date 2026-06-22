package io.github.peterdowdy.litebfx.bam;

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
 * Unit tests for {@link BamScanBuilder#pushPredicates} and
 * {@link BamScanBuilder#pruneColumns}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>Equality predicate on {@code referenceName} (match / attribute-mismatch)</li>
 *   <li>Case-insensitive {@code referenceName} column matching (Spark 4.x lowercasing)</li>
 *   <li>All four range predicate types on {@code start}</li>
 *   <li>Range predicates when no referenceName equality is set (must be returned unhandled)</li>
 *   <li>Range predicate with wrong attribute (must be returned unhandled)</li>
 *   <li>{@code And}-wrapped compound predicate unwrapping</li>
 *   <li>Column pruning — with and without {@code attributes} in the required schema</li>
 * </ul>
 *
 * <p>Note: range predicates ({@code start} comparisons) are always returned unhandled so
 * Spark post-filters for exactness — BAI overlap queries may return reads that fall
 * partially outside the requested range.  Only {@code referenceName} equality is pushed.
 */
class BamScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.bam"));
    }

    private static CaseInsensitiveStringMap samOpts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.bam", "columnNames", "sam"));
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
        BamScanBuilder b = new BamScanBuilder(opts());
        assertEquals(0, b.pushPredicates(new Predicate[0]).length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — referenceName equality (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToReferenceName_handledNotReturned() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate refPred = eq("referenceName", "chr1");
        assertEquals(0, b.pushPredicates(new Predicate[]{refPred}).length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — case-insensitive referenceName (Spark 4.x lowercasing)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_referenceNameLowercase_handled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate refPred = eq("referencename", "chr1");
        assertEquals(0, b.pushPredicates(new Predicate[]{refPred}).length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_refNameLowercasePlusGte_refNameHandledRangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] preds = {eq("referencename", "chr1"), gte("start", 100L)};
        assertEquals(1, b.pushPredicates(preds).length, "start range should be returned unhandled");
        assertEquals(1, b.pushedPredicates().length, "only referenceName equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — equality on wrong attribute
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToWrongAttribute_refNameNotExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("readName", "read1")});
        assertEquals(1, unhandled.length);
        assertEquals(0, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range without referenceName (must be returned unhandled)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_rangeFiltersNoReferenceName_allReturnedUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{gte("start", 100L), lte("start", 1000L)});
        assertEquals(2, unhandled.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range types (with referenceName):
    // referenceName equality is pushed; range is returned unhandled for Spark post-filtering
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_refNamePlusGte_refNamePushedRangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), gte("start", 100L)});
        assertEquals(1, unhandled.length, "start range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only referenceName equality is pushed");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_refNamePlusGt_refNamePushedRangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), gt("start", 99L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_refNamePlusLte_refNamePushedRangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), lte("start", 1000L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_refNamePlusLt_refNamePushedRangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), lt("start", 1001L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — range with wrong attribute (referenceName present)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_refNamePlusGteWrongAttr_rangeUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), gte("mapQ", 30L)});
        assertEquals(1, unhandled.length);
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — And-wrapped compound predicate
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_andWrapped_refNamePushedStartUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate refPred  = eq("referenceName", "chr1");
        Predicate startGte = gte("start", 100L);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{new And(refPred, startGte)});
        assertEquals(1, unhandled.length, "start range from And should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only referenceName equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — all four range types together with referenceName
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_allRangeTypes_onlyRefNamePushed() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Predicate[] preds = {
                eq("referenceName", "chrX"),
                gte("start", 100L),
                gt("start", 99L),
                lte("start", 200L),
                lt("start", 201L)
        };
        assertEquals(4, b.pushPredicates(preds).length, "all four range predicates should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only referenceName equality is pushed");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedPredicates — reflects handled set (equality only)
    // -------------------------------------------------------------------------

    @Test
    void pushedPredicates_returnsHandledPredicates() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushPredicates(new Predicate[]{eq("referenceName", "chr1")});
        assertEquals(1, b.pushedPredicates().length);
    }

    // -------------------------------------------------------------------------
    // pruneColumns — with attributes column (includeAttributes = true)
    // -------------------------------------------------------------------------

    @Test
    void pruneColumns_withAttributesColumn_includesAttributes() {
        BamScanBuilder b = new BamScanBuilder(opts());
        StructType withAttrs = new StructType()
                .add("readName", DataTypes.StringType)
                .add("attributes", DataTypes.createMapType(
                        DataTypes.StringType, DataTypes.StringType));
        b.pruneColumns(withAttrs);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pruneColumns — without attributes column (includeAttributes = false)
    // -------------------------------------------------------------------------

    @Test
    void pruneColumns_withoutAttributesColumn_excludesAttributes() {
        BamScanBuilder b = new BamScanBuilder(opts());
        StructType withoutAttrs = new StructType()
                .add("readName", DataTypes.StringType)
                .add("start", DataTypes.IntegerType);
        b.pruneColumns(withoutAttrs);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — SAM column-name mode (columnNames=sam): pushdown on rname/pos
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_samMode_rnamePlusPos_rnamePushedPosUnhandled() {
        BamScanBuilder b = new BamScanBuilder(samOpts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("rname", "chr1"), gte("pos", 100L)});
        assertEquals(1, unhandled.length, "pos range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "rname equality should be pushed in SAM mode");
        assertNotNull(b.build());
    }

    @Test
    void pushPredicates_samMode_descriptiveNamesNotRecognized() {
        // In SAM mode the DataFrame uses rname/pos, so the descriptive names are not the
        // active alignment columns and must not be treated as pushable region predicates.
        BamScanBuilder b = new BamScanBuilder(samOpts());
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{eq("referenceName", "chr1"), gte("start", 100L)});
        assertEquals(2, unhandled.length, "descriptive names should be returned unhandled in SAM mode");
        assertEquals(0, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // CRAM variant — isCram=true constructor path
    // -------------------------------------------------------------------------

    @Test
    void cramVariant_pushPredicates_sameLogicAsBam() {
        BamScanBuilder b = new BamScanBuilder(opts(), true);
        Predicate[] preds = {eq("referenceName", "chr1"), gte("start", 1000L)};
        assertEquals(1, b.pushPredicates(preds).length, "start range should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "only referenceName equality is pushed");
        assertNotNull(b.build());
    }
}
