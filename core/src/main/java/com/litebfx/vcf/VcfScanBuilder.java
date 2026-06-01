package com.litebfx.vcf;

import com.litebfx.fasta.FastaScanBuilder;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link VcfScan} for a VCF DataSource V2 read.
 *
 * <h3>Predicate pushdown</h3>
 * Recognized predicates ({@code chrom} equality + {@code pos} range) are extracted
 * for tabix-based optimization.  Unrecognized predicates are returned unhandled so
 * Spark applies a post-scan pass for them.
 */
public class VcfScanBuilder
        implements ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownLimit,
                   SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(VcfScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = VcfSchema.SCHEMA;

    private String pushedChrom = null;
    private int    pushedStart = 1;
    private int    pushedEnd   = Integer.MAX_VALUE;
    private int    pushedLimit = Integer.MAX_VALUE;
    private Predicate[] handledPredicates = new Predicate[0];

    VcfScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownV2Filters
    // -------------------------------------------------------------------------

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        log.trace("pushPredicates(predicates={})", (Object) predicates);
        Predicate[] flat = FastaScanBuilder.flatten(predicates);

        // First pass: extract chrom equality
        String chrom = null;
        Predicate chromPredicate = null;
        for (Predicate p : flat) {
            if (FastaScanBuilder.isColumnEquality(p, "chrom")) {
                chrom = String.valueOf(FastaScanBuilder.literalValue(p));
                chromPredicate = p;
                break;
            }
        }

        List<Predicate> handled = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();

        if (chrom != null) {
            handled.add(chromPredicate);
            int start = 1;
            int end   = Integer.MAX_VALUE;
            // Second pass: extract pos range for the tabix index query.
            // Range predicates are returned unhandled so Spark post-filters for exactness;
            // tabix overlap queries may return records that fall partially outside the range.
            for (Predicate p : flat) {
                if (p == chromPredicate) continue;
                if (FastaScanBuilder.isRangeComparison(p)
                        && "pos".equals(FastaScanBuilder.columnName(p))) {
                    String op = p.name();
                    int val = ((Number) FastaScanBuilder.literalValue(p)).intValue();
                    if (">=".equals(op)) start = val;
                    else if (">".equals(op)) start = val + 1;
                    else if ("<=".equals(op)) end = val;
                    else if ("<".equals(op)) end = val - 1;
                    unhandled.add(p);
                } else {
                    unhandled.add(p);
                }
            }
            pushedChrom = chrom;
            pushedStart = start;
            pushedEnd   = end;
        } else {
            for (Predicate p : flat) unhandled.add(p);
        }

        log.trace("pushPredicates() extracted chrom={} start={} end={}", pushedChrom, pushedStart, pushedEnd);
        handledPredicates = handled.toArray(new Predicate[0]);
        return unhandled.toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates() {
        return handledPredicates;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownLimit
    // -------------------------------------------------------------------------

    @Override
    public boolean pushLimit(int limit) {
        log.trace("pushLimit({})", limit);
        this.pushedLimit = limit;
        return false;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownRequiredColumns
    // -------------------------------------------------------------------------

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    // -------------------------------------------------------------------------
    // ScanBuilder
    // -------------------------------------------------------------------------

    @Override
    public Scan build() {
        log.trace("build()");
        return new VcfScan(options, requiredSchema, pushedChrom, pushedStart, pushedEnd, pushedLimit);
    }
}
