package com.litebfx;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Represents a BAM/SAM file (or directory/glob of files) as a Spark table.
 *
 * <p>Capabilities: {@code BATCH_READ} only.
 */
public class BamTable implements Table, SupportsRead {

    private final Map<String, String> properties;
    private final boolean isCram;

    BamTable(Map<String, String> properties) {
        this(properties, false);
    }

    BamTable(Map<String, String> properties, boolean isCram) {
        this.properties = properties;
        this.isCram = isCram;
    }

    @Override
    public String name() {
        return properties.getOrDefault("path", isCram ? "cram" : "bam");
    }

    @Override
    public StructType schema() {
        return BamSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new BamScanBuilder(options, isCram);
    }
}
