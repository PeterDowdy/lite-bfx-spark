package com.litebfx.bam;

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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link BamScan} for a BAM/SAM DataSource V2 read.
 *
 * <h3>Predicate pushdown</h3>
 * Recognized filters ({@code referenceName} equality + {@code start} range) are
 * extracted and stored for use in {@link BamScan#planInputPartitions()}.  All
 * filters are returned as <em>unhandled</em> from {@link #pushFilters} so that
 * Spark always applies a post-scan safety pass — this keeps results correct for
 * SAM files and BAM files without a BAI index, where the reader does a full scan.
 *
 * <h3>Column pruning</h3>
 * If {@code attributes} is absent from the required schema, the attributes map
 * is not built during row conversion (avoiding per-record tag parsing overhead).
 */
public class BamScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(BamScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private final boolean isCram;

    private StructType requiredSchema = BamSchema.SCHEMA;
    private boolean includeAttributes = true;

    // Extracted region (null referenceName means "no region filter")
    private String pushedReferenceName = null;
    private int pushedStart = 1;
    private int pushedEnd = Integer.MAX_VALUE;

    BamScanBuilder(CaseInsensitiveStringMap options) {
        this(options, false);
    }

    BamScanBuilder(CaseInsensitiveStringMap options, boolean isCram) {
        log.trace("BamScanBuilder(isCram={})", isCram);
        this.options = options;
        this.isCram = isCram;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownFilters
    // -------------------------------------------------------------------------

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        log.trace("pushFilters(filters={})", (Object) filters);
        // Extract filters we can use for BAI-based optimization.
        String refName = null;
        int rangeStart = 1;
        int rangeEnd = Integer.MAX_VALUE;

        for (Filter f : filters) {
            if (f instanceof EqualTo eq && "referenceName".equals(eq.attribute())) {
                refName = String.valueOf(eq.value());
            }
        }
        if (refName != null) {
            for (Filter f : filters) {
                if (f instanceof GreaterThanOrEqual gte && "start".equals(gte.attribute())) {
                    rangeStart = ((Number) gte.value()).intValue();
                } else if (f instanceof GreaterThan gt && "start".equals(gt.attribute())) {
                    rangeStart = ((Number) gt.value()).intValue() + 1;
                } else if (f instanceof LessThanOrEqual lte && "start".equals(lte.attribute())) {
                    rangeEnd = ((Number) lte.value()).intValue();
                } else if (f instanceof LessThan lt && "start".equals(lt.attribute())) {
                    rangeEnd = ((Number) lt.value()).intValue() - 1;
                }
            }
            pushedReferenceName = refName;
            pushedStart = rangeStart;
            pushedEnd = rangeEnd;
        }
        log.trace("pushFilters() extracted referenceName={} start={} end={}", pushedReferenceName, pushedStart, pushedEnd);

        // Return ALL filters as unhandled so Spark post-filters for correctness.
        // BAI-based optimization in BamScan is transparent to Spark's planner.
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        log.trace("pushedFilters()");
        return new Filter[0];
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownRequiredColumns
    // -------------------------------------------------------------------------

    @Override
    public void pruneColumns(StructType requiredSchema) {
        log.trace("pruneColumns(requiredSchema={})", requiredSchema);
        this.requiredSchema = requiredSchema;
        boolean found = false;
        for (StructField f : requiredSchema.fields()) {
            if ("attributes".equals(f.name())) {
                found = true;
                break;
            }
        }
        this.includeAttributes = found;
        log.trace("pruneColumns() includeAttributes={}", includeAttributes);
    }

    // -------------------------------------------------------------------------
    // ScanBuilder
    // -------------------------------------------------------------------------

    @Override
    public Scan build() {
        log.trace("build()");
        return new BamScan(options, requiredSchema, includeAttributes,
                           pushedReferenceName, pushedStart, pushedEnd, isCram);
    }
}
