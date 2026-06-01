package com.litebfx.fastq;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link FastqScan}.
 *
 * <p>FASTQ has no index format, so neither filter pushdown nor column pruning
 * are implemented. All records are read and Spark evaluates predicates row-by-row.
 */
public class FastqScanBuilder implements SupportsPushDownLimit {

    private static final Logger log = LoggerFactory.getLogger(FastqScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private int pushedLimit = Integer.MAX_VALUE;

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
    public Scan build() {
        log.trace("build()");
        return new FastqScan(options, pushedLimit);
    }
}
