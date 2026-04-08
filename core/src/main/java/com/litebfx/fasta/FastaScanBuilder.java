package com.litebfx.fasta;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link FastaScan}.
 *
 * <h3>Filter pushdown</h3>
 * An {@code EqualTo("name", value)} filter is used to prune which contig
 * partitions are planned — only the matching contig is scheduled as a Spark
 * partition, avoiding reads of unneeded contigs when a FAI index is present.
 * All filters are returned as unhandled so Spark applies a post-scan safety pass.
 *
 * <h3>Column pruning</h3>
 * Column pruning is supported so that executors can skip base-string
 * decoding when {@code sequence} is not in the required schema.
 */
public class FastaScanBuilder
        implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(FastaScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = FastaSchema.SCHEMA;
    private String pushedName = null;

    FastaScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("FastaScanBuilder(options={})", options);
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownFilters
    // -------------------------------------------------------------------------

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        log.trace("pushFilters(filters={})", (Object) filters);
        for (Filter f : filters) {
            if (f instanceof EqualTo eq && "name".equals(eq.attribute())) {
                pushedName = String.valueOf(eq.value());
                log.trace("pushFilters() extracted name={}", pushedName);
            }
        }
        // Return all filters as unhandled — Spark applies a post-scan safety pass.
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
        log.trace("pruneColumns(requiredSchema={})", requiredSchema);
        this.requiredSchema = requiredSchema;
    }

    // -------------------------------------------------------------------------
    // ScanBuilder
    // -------------------------------------------------------------------------

    @Override
    public Scan build() {
        log.trace("build()");
        return new FastaScan(options, requiredSchema, pushedName);
    }
}
