package io.github.peterdowdy.litebfx.fastq;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link FastqScan}.
 *
 * <p>FASTQ has no index format, so filter pushdown and data-column pruning are not implemented.
 * It implements {@link SupportsPushDownRequiredColumns} solely so Spark can signal a request for
 * the hidden {@code _metadata} column (which it passes through {@link #pruneColumns}); all data
 * columns are always read and Spark evaluates predicates row-by-row.
 */
public class FastqScanBuilder implements SupportsPushDownLimit, SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(FastqScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private int pushedLimit = Integer.MAX_VALUE;
    private boolean includeFileMetadata = false;

    FastqScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("FastqScanBuilder(options={})", options);
        this.options = options;
    }

    @Override
    public boolean pushLimit(int limit) {
        log.trace("pushLimit({})", limit);
        this.pushedLimit = limit;
        return false;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        log.trace("pruneColumns(requiredSchema={})", requiredSchema);
        this.includeFileMetadata = io.github.peterdowdy.litebfx.FileMetadata.isRequested(requiredSchema);
    }

    @Override
    public Scan build() {
        log.trace("build()");
        return new FastqScan(options, pushedLimit, includeFileMetadata);
    }
}
