package com.litebfx.bed;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link BedScan} for a BED DataSource V2 read.
 *
 * <h3>Predicate pushdown</h3>
 * Recognized filters ({@code chrom} equality + {@code chromStart}/{@code chromEnd}
 * range) are extracted for tabix-based optimization.  All filters are returned as
 * unhandled so Spark always applies a post-scan safety pass.
 */
public class BedScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(BedScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = BedSchema.SCHEMA;

    private String pushedChrom   = null;
    private long   pushedStart   = 0L;
    private long   pushedEnd     = Long.MAX_VALUE;

    BedScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownFilters
    // -------------------------------------------------------------------------

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        log.trace("pushFilters(filters={})", (Object) filters);
        String chrom = null;
        long start = 0L;
        long end   = Long.MAX_VALUE;

        for (Filter f : filters) {
            if (f instanceof EqualTo eq && "chrom".equals(eq.attribute())) {
                chrom = String.valueOf(eq.value());
            }
        }
        if (chrom != null) {
            for (Filter f : filters) {
                if (f instanceof GreaterThanOrEqual gte && "chromStart".equals(gte.attribute())) {
                    start = ((Number) gte.value()).longValue();
                } else if (f instanceof GreaterThan gt && "chromStart".equals(gt.attribute())) {
                    start = ((Number) gt.value()).longValue() + 1;
                } else if (f instanceof LessThanOrEqual lte && "chromEnd".equals(lte.attribute())) {
                    end = ((Number) lte.value()).longValue();
                } else if (f instanceof LessThan lt && "chromEnd".equals(lt.attribute())) {
                    end = ((Number) lt.value()).longValue() - 1;
                }
            }
            pushedChrom = chrom;
            pushedStart = start;
            pushedEnd   = end;
        }
        log.trace("pushFilters() extracted chrom={} start={} end={}", pushedChrom, pushedStart, pushedEnd);
        // Return ALL filters as unhandled; Spark post-filters for correctness.
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
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
        return new BedScan(options, requiredSchema, pushedChrom, pushedStart, pushedEnd);
    }
}
