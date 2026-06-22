package io.github.peterdowdy.litebfx.fasta;

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

/** Represents a FASTA file (or glob of files) as a Spark table. */
public class FastaTable implements Table, SupportsRead, SupportsMetadataColumns {

    private static final Logger log = LoggerFactory.getLogger(FastaTable.class);

    private final Map<String, String> properties;

    FastaTable(Map<String, String> properties) {
        log.trace("FastaTable(properties={})", properties);
        this.properties = properties;
    }

    @Override
    public String name() {
        return properties.getOrDefault("path", "fasta");
    }

    @Override
    public StructType schema() {
        return FastaSchema.SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("newScanBuilder(options={})", options);
        return new FastaScanBuilder(options);
    }

    @Override
    public MetadataColumn[] metadataColumns() {
        return FileMetadata.COLUMNS;
    }
}
