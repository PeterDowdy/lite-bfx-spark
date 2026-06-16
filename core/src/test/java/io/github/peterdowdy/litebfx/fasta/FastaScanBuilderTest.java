package io.github.peterdowdy.litebfx.fasta;

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
 * Unit tests for {@link FastaScanBuilder#pushPredicates}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>Equality predicate on {@code name} (match / attribute-mismatch / non-equality type)</li>
 *   <li>{@code And}-wrapped predicate unwrapping</li>
 *   <li>Column pruning via {@link FastaScanBuilder#pruneColumns}</li>
 * </ul>
 */
class FastaScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.fa"));
    }

    /** Creates a V2 equality predicate: col = value. */
    private static Predicate eq(String col, String val) {
        return new Predicate("=", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.StringType)
        });
    }

    /** Creates a V2 greater-than predicate: col > value. */
    private static Predicate gt(String col, long val) {
        return new Predicate(">", new Expression[]{
                FieldReference.apply(col),
                LiteralValue.apply(val, DataTypes.LongType)
        });
    }

    // -------------------------------------------------------------------------
    // pushPredicates — no predicates
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_empty_returnsEmptyAndBuilds() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate[] result = b.pushPredicates(new Predicate[0]);
        assertEquals(0, result.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — equality on name (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToName_handledAndNotReturned() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate namePred = eq("name", "chr1");
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{namePred});
        assertEquals(0, unhandled.length, "name equality should be handled");
        assertEquals(1, b.pushedPredicates().length, "name equality should be in pushedPredicates");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — equality on wrong attribute
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_equalToWrongAttribute_notHandled() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate wrongAttr = eq("length", "100");
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{wrongAttr});
        assertEquals(1, unhandled.length, "wrong attribute should remain unhandled");
        assertEquals(0, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — non-equality type (range predicate)
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_rangePredicateOnName_notHandled() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate rangePred = gt("length", 100L);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{rangePred});
        assertEquals(1, unhandled.length, "range predicate should remain unhandled");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — mixed predicates
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_mixedPredicates_onlyNameHandled() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate namePred = eq("name", "chr2");
        Predicate otherPred = gt("length", 100L);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{namePred, otherPred});
        assertEquals(1, unhandled.length, "only the non-name predicate should be unhandled");
        assertEquals(1, b.pushedPredicates().length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushPredicates — And-wrapped predicate
    // -------------------------------------------------------------------------

    @Test
    void pushPredicates_andWrapped_nameExtracted() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Predicate namePred = eq("name", "chrM");
        Predicate otherPred = gt("length", 1000L);
        Predicate andPred = new And(namePred, otherPred);
        Predicate[] unhandled = b.pushPredicates(new Predicate[]{andPred});
        // namePred is handled, otherPred is unhandled
        assertEquals(1, unhandled.length, "non-name predicate from And should be unhandled");
        assertEquals(1, b.pushedPredicates().length, "name predicate from And should be handled");
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedPredicates — reflects handled predicates
    // -------------------------------------------------------------------------

    @Test
    void pushedPredicates_returnsHandledPredicates() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        b.pushPredicates(new Predicate[]{eq("name", "chrM")});
        assertEquals(1, b.pushedPredicates().length);
    }

    // -------------------------------------------------------------------------
    // pruneColumns — accepted without error
    // -------------------------------------------------------------------------

    @Test
    void pruneColumns_reducedSchema_buildsSuccessfully() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        StructType reduced = new StructType()
                .add("name", DataTypes.StringType)
                .add("length", DataTypes.LongType);
        b.pruneColumns(reduced);
        assertNotNull(b.build());
    }
}
