package com.litebfx.vcf;

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
 * Builds a {@link VcfScan} for a VCF DataSource V2 read.
 *
 * <h3>Predicate pushdown</h3>
 * Recognized filters ({@code chrom} equality + {@code pos} range) are extracted
 * for tabix-based optimization.  All filters are returned as unhandled so Spark
 * always applies a post-scan safety pass.
 */
public class VcfScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(VcfScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = VcfSchema.SCHEMA;

    private String pushedChrom = null;
    private int    pushedStart = 1;
    private int    pushedEnd   = Integer.MAX_VALUE;

    VcfScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownFilters
    // -------------------------------------------------------------------------

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        log.trace("pushFilters(filters={})", (Object) filters);
        String chrom = null;
        int start = 1;
        int end   = Integer.MAX_VALUE;

        for (Filter f : filters) {
            if (f instanceof EqualTo eq && "chrom".equals(eq.attribute())) {
                chrom = String.valueOf(eq.value());
            }
        }
        if (chrom != null) {
            for (Filter f : filters) {
                if (f instanceof GreaterThanOrEqual gte && "pos".equals(gte.attribute())) {
                    start = ((Number) gte.value()).intValue();
                } else if (f instanceof GreaterThan gt && "pos".equals(gt.attribute())) {
                    start = ((Number) gt.value()).intValue() + 1;
                } else if (f instanceof LessThanOrEqual lte && "pos".equals(lte.attribute())) {
                    end = ((Number) lte.value()).intValue();
                } else if (f instanceof LessThan lt && "pos".equals(lt.attribute())) {
                    end = ((Number) lt.value()).intValue() - 1;
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
        return new VcfScan(options, requiredSchema, pushedChrom, pushedStart, pushedEnd);
    }
}
