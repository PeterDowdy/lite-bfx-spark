package io.github.peterdowdy.litebfx.bed;

import io.github.peterdowdy.litebfx.FileMetadata;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
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
 * Represents a BED file (or directory/glob) as a Spark table.
 */
public class BedTable implements Table, SupportsRead, SupportsMetadataColumns {

    private static final Logger log = LoggerFactory.getLogger(BedTable.class);

    private final Map<String, String> properties;

    BedTable(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String name() {
        return properties.getOrDefault("path", "bed");
    }

    @Override
    public StructType schema() {
        return BedSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("newScanBuilder(options={})", options);
        return new BedScanBuilder(options);
    }

    @Override
    public MetadataColumn[] metadataColumns() {
        return FileMetadata.COLUMNS;
    }
}
