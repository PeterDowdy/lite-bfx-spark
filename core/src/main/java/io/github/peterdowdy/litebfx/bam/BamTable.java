package io.github.peterdowdy.litebfx.bam;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataType;
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
public class BamTable implements Table, SupportsRead, SupportsMetadataColumns {

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
        return BamSchema.fromOptions(properties);
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

    @Override
    public MetadataColumn[] metadataColumns() {
        log.trace("metadataColumns()");
        return new MetadataColumn[]{FileMetadataColumn.INSTANCE};
    }

    /**
     * The hidden {@value BamSchema#METADATA_COLUMN_NAME} column, surfaced to Spark via
     * {@link SupportsMetadataColumns}. When referenced in a query, Spark passes it into
     * {@link BamScanBuilder#pruneColumns} and the partition reader appends its value.
     */
    private enum FileMetadataColumn implements MetadataColumn {
        INSTANCE;

        @Override
        public String name() {
            return BamSchema.METADATA_COLUMN_NAME;
        }

        @Override
        public DataType dataType() {
            return BamSchema.FILE_METADATA_TYPE;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public String comment() {
            return "File metadata: file_path, file_name, file_size, file_modification_time";
        }
    }
}
