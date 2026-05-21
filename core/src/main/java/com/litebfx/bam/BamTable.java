package com.litebfx.bam;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Represents a BAM/SAM file (or directory/glob of files) as a Spark table.
 *
 * <p>Capabilities: {@code BATCH_READ} only.
 */
public class BamTable implements Table, SupportsRead {

    private static final Logger log = LoggerFactory.getLogger(BamTable.class);

    private final Map<String, String> properties;
    private final boolean isCram;

    BamTable(Map<String, String> properties) {
        this(properties, false);
    }

    BamTable(Map<String, String> properties, boolean isCram) {
        log.trace("BamTable(isCram={}, properties={})", isCram, properties);
        this.properties = properties;
        this.isCram = isCram;
    }

    @Override
    public String name() {
        String n = properties.getOrDefault("path", isCram ? "cram" : "bam");
        log.trace("name() -> {}", n);
        return n;
    }

    @Override
    public StructType schema() {
        log.trace("schema()");
        return BamSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        log.trace("capabilities()");
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("newScanBuilder(options={})", options);
        return new BamScanBuilder(options, isCram);
    }
}
