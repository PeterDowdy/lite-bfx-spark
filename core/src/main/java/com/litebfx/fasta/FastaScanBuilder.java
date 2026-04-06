package com.litebfx.fasta;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link FastaScan}.
 *
 * <p>Column pruning is supported so that executors can skip base-string
 * decoding when {@code sequence} is not in the required schema.
 * No filter pushdown — FASTA has no genomic-coordinate index for contig bodies.
 */
public class FastaScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(FastaScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = FastaSchema.SCHEMA;

    FastaScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("FastaScanBuilder(options={})", options);
        this.options = options;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        log.trace("pruneColumns(requiredSchema={})", requiredSchema);
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        log.trace("build()");
        return new FastaScan(options, requiredSchema);
    }
}
