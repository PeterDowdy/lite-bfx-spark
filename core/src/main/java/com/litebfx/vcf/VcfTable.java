package com.litebfx.vcf;

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
 * Represents a VCF/BCF file (or directory/glob) as a Spark table.
 */
public class VcfTable implements Table, SupportsRead {

    private static final Logger log = LoggerFactory.getLogger(VcfTable.class);

    private final Map<String, String> properties;

    VcfTable(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String name() {
        return properties.getOrDefault("path", "vcf");
    }

    @Override
    public StructType schema() {
        return VcfSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("newScanBuilder(options={})", options);
        return new VcfScanBuilder(options);
    }
}
