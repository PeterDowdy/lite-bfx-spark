package io.github.peterdowdy.litebfx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

/**
 * Shared definition and producer for the hidden {@value #COLUMN_NAME} column exposed by every
 * lite-bfx-spark file format (BAM/SAM/CRAM, FASTA, FASTQ, BED, VCF/BCF).
 *
 * <p>The column is a struct compatible with Spark's built-in
 * <a href="https://docs.databricks.com/en/ingestion/file-metadata-column.html">file-source
 * metadata column</a>, using identical field names and types so queries referencing
 * {@code _metadata.file_path} (etc.) port over unchanged. It is not part of any format's declared
 * schema — Spark surfaces it via {@link org.apache.spark.sql.connector.catalog.SupportsMetadataColumns}
 * and passes it into {@code SupportsPushDownRequiredColumns#pruneColumns} when referenced, at which
 * point the scan appends it to {@link #withMetadata(StructType)} and the partition reader appends
 * its value with {@link #appendTo(InternalRow, InternalRow)}.
 *
 * <p>The {@code file_block_start}, {@code file_block_length}, and {@code row_index} sub-fields of
 * Spark's built-in column are intentionally omitted: byte ranges and row indices have no stable
 * meaning under the genomic (per-reference / per-container / per-contig) partitioning used here.
 * One field is <em>added</em> beyond Spark's built-in column: {@code index_path}, the index file
 * (BAI/CRAI, FAI, tabix/CSI) that was used to locate this partition's data, or null when the read
 * did not use an index (e.g. FASTQ, full-file scans, or {@code useIndex=false}).
 */
public final class FileMetadata {

    /** Name of the hidden metadata column. */
    public static final String COLUMN_NAME = "_metadata";

    /** Struct type of the {@value #COLUMN_NAME} column. */
    public static final StructType TYPE = new StructType()
        .add("file_path",              DataTypes.StringType,    false)
        .add("file_name",              DataTypes.StringType,    false)
        .add("file_size",              DataTypes.LongType,      false)
        .add("file_modification_time", DataTypes.TimestampType, false)
        .add("index_path",             DataTypes.StringType,    true); // null when no index was used

    /** The metadata column descriptor returned from {@code Table.metadataColumns()}. */
    public static final MetadataColumn[] COLUMNS = {new FileMetadataColumn()};

    /** True when the required schema (from {@code pruneColumns}) references {@value #COLUMN_NAME}. */
    public static boolean isRequested(StructType requiredSchema) {
        for (StructField f : requiredSchema.fields()) {
            if (COLUMN_NAME.equals(f.name())) return true;
        }
        return false;
    }

    /** Appends the {@value #COLUMN_NAME} struct field to a format's data schema. */
    public static StructType withMetadata(StructType dataSchema) {
        return dataSchema.add(COLUMN_NAME, TYPE, false);
    }

    /**
     * Builds the {@value #COLUMN_NAME} struct value for the file at {@code path}. Field order
     * matches {@link #TYPE}: file_path, file_name, file_size, file_modification_time, index_path.
     * The modification time is stored as microseconds since the epoch (Spark {@code TimestampType}).
     *
     * @param indexPath the index file used to locate this partition's data, or null if none
     */
    public static InternalRow row(Configuration conf, String path, String indexPath) throws IOException {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(p);
        return new GenericInternalRow(new Object[]{
            UTF8String.fromString(path),
            UTF8String.fromString(p.getName()),
            status.getLen(),
            status.getModificationTime() * 1000L,
            indexPath == null ? null : UTF8String.fromString(indexPath),
        });
    }

    /**
     * Returns {@code dataRow} with the {@value #COLUMN_NAME} struct appended as one trailing column,
     * matching {@link #withMetadata(StructType)}.
     */
    public static InternalRow appendTo(InternalRow dataRow, InternalRow metadataStruct) {
        return new JoinedRow(dataRow, new GenericInternalRow(new Object[]{metadataStruct}));
    }

    private FileMetadata() {}

    /** Plain singleton (not an enum: {@link MetadataColumn#name()} clashes with {@code Enum.name()}). */
    private static final class FileMetadataColumn implements MetadataColumn {
        @Override
        public String name() {
            return COLUMN_NAME;
        }

        @Override
        public DataType dataType() {
            return TYPE;
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
