package io.github.peterdowdy.litebfx.fastq;

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

/** Represents a FASTQ file (or directory/glob of files) as a Spark table. */
public class FastqTable implements Table, SupportsRead {

    private static final Logger log = LoggerFactory.getLogger(FastqTable.class);

    private final Map<String, String> properties;

    FastqTable(Map<String, String> properties) {
        log.trace("FastqTable(properties={})", properties);
        this.properties = properties;
    }

    @Override
    public String name() {
        return properties.getOrDefault("path", "fastq");
    }

    @Override
    public StructType schema() {
        return FastqSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("newScanBuilder(options={})", options);
        return new FastqScanBuilder(options);
    }
}
