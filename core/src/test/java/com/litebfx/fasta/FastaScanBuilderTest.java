package com.litebfx.fasta;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FastaScanBuilder#pushFilters}.
 *
 * <p>Drives the builder directly (no SparkSession) to exercise all branch
 * combinations in the predicate-pushdown logic:
 * <ul>
 *   <li>EqualTo on {@code name} (match / attribute-mismatch / non-EqualTo type)</li>
 *   <li>Column pruning via {@link FastaScanBuilder#pruneColumns}</li>
 * </ul>
 */
class FastaScanBuilderTest {

    private static CaseInsensitiveStringMap opts() {
        return new CaseInsensitiveStringMap(Map.of("path", "test.fa"));
    }

    // -------------------------------------------------------------------------
    // pushFilters — no filters
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_empty_returnsEmptyAndBuilds() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Filter[] result = b.pushFilters(new Filter[0]);
        assertEquals(0, result.length);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on name (success path)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToName_returnsAllFiltersUnhandled() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Filter[] filters = {new EqualTo("name", "chr1")};
        assertSame(filters, b.pushFilters(filters));
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — EqualTo on wrong attribute (type matches, attribute doesn't)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_equalToWrongAttribute_nameNotExtracted() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        // EqualTo type matches but attribute != "name" → pushedName stays null
        b.pushFilters(new Filter[]{new EqualTo("length", "100")});
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — non-EqualTo filter type (instanceof check fails)
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_nonEqualToType_ignored() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        // GreaterThan is not EqualTo → instanceof check = false → nameNotExtracted
        b.pushFilters(new Filter[]{new GreaterThan("length", 100L)});
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushFilters — multiple filters including one matching EqualTo("name")
    // -------------------------------------------------------------------------

    @Test
    void pushFilters_mixedFilters_onlyNameExtracted() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        Filter[] filters = {
                new GreaterThan("length", 100L),
                new EqualTo("name", "chr2"),
                new EqualTo("someOtherField", "value")
        };
        Filter[] returned = b.pushFilters(filters);
        assertSame(filters, returned);
        assertNotNull(b.build());
    }

    // -------------------------------------------------------------------------
    // pushedFilters — always empty
    // -------------------------------------------------------------------------

    @Test
    void pushedFilters_alwaysReturnsEmpty() {
        FastaScanBuilder b = new FastaScanBuilder(opts());
        b.pushFilters(new Filter[]{new EqualTo("name", "chrM")});
        assertEquals(0, b.pushedFilters().length);
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
