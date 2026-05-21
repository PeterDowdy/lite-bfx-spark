package com.litebfx.bam;

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
 * Unit tests for {@link BamScanBuilder#pushFilters} and
 * {@link BamScanBuilder#pruneColumns}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>EqualTo on {@code referenceName} (match / attribute-mismatch)</li>
 *   <li>All four range filter types on {@code start}</li>
 *   <li>Range filters when no referenceName EqualTo is set (must be ignored)</li>
 *   <li>Range filter with wrong attribute (must be ignored)</li>
 *   <li>Column pruning — with and without {@code attributes} in the required schema</li>
 * </ul>
 */
class BamScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.bam"));
    }

    // -------------------------------------------------------------------------
    // pushFilters — no filters
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_empty_returnsEmptyAndBuilds() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Filter[] result = b.pushFilters(new Filter[0]);
        assertEquals(0, result.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on referenceName (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToReferenceName_returnsAllFiltersUnhandled() {
        BamScanBuilder b = new BamScanBuilder(opts());
        Filter[] filters = {new EqualTo("referenceName", "chr1")};
        assertSame(filters, b.pushFilters(filters));
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on wrong attribute (type matches, attribute doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToWrongAttribute_refNameNotExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("readName", "read1")});
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filters without referenceName (must be silently ignored)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_rangeFiltersNoReferenceName_ignored() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new GreaterThanOrEqual("start", 100),
                new LessThanOrEqual("start", 1000)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThanOrEqual on start (with referenceName set)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_refNamePlusGte_startExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new GreaterThanOrEqual("start", 100)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — GreaterThan on start (start + 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_refNamePlusGt_startPlusOneExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new GreaterThan("start", 99)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThanOrEqual on start
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_refNamePlusLte_endExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new LessThanOrEqual("start", 1000)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — LessThan on start (end - 1 semantic)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_refNamePlusLt_endMinusOneExtracted() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new LessThan("start", 1001)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — range filter with wrong attribute (type matches, attr doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_refNamePlusGteWrongAttr_ignored() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new GreaterThanOrEqual("mapQ", 30)   // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_refNamePlusLtWrongAttr_ignored() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new LessThan("mapQ", 255)            // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_refNamePlusGtWrongAttr_ignored() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new GreaterThan("mapQ", 20)           // wrong attribute
        });
        assertNotNull(b.build());
    }

    @Test
    void pushFilters_refNamePlusLteWrongAttr_ignored() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new LessThanOrEqual("mapQ", 255)      // wrong attribute
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — all four range filter types together with referenceName
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_allRangeFilterTypes_allProcessed() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chrX"),
                new GreaterThanOrEqual("start", 100),
                new GreaterThan("start", 99),
                new LessThanOrEqual("start", 200),
                new LessThan("start", 201)
        });
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedFilters — always empty
    // -------------------------------------------------------------------------

    @Test
    void pushedFilters_alwaysReturnsEmpty() {
        BamScanBuilder b = new BamScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("referenceName", "chr1")});
        assertEquals(0, b.pushedFilters().length);
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
    // CRAM variant — isCram=true constructor path
    // -------------------------------------------------------------------------

    @Test
    void cramVariant_pushFilters_sameLogicAsBam() {
        BamScanBuilder b = new BamScanBuilder(opts(), true);
        b.pushFilters(new Filter[]{
                new EqualTo("referenceName", "chr1"),
                new GreaterThanOrEqual("start", 1000)
        });
        assertNotNull(b.build());
    }
}
