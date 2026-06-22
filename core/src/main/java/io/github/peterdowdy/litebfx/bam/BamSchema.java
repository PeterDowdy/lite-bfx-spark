package io.github.peterdowdy.litebfx.bam;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Spark schema for BAM/SAM/CRAM: column names, types, and nullability for all alignment fields.
 *
 * <p>Coordinate note: {@code start}/{@code pos} and {@code mateStart}/{@code pnext} are
 * <b>1-based</b> positions as defined by the SAM specification. This differs from BED
 * ({@code chromStart} is 0-based), so take care when joining BAM and BED data on genomic
 * coordinates.
 *
 * <p>Attributes note: {@code attributes} is a {@code map<string,string>} keyed by the
 * two-character tag. Each value is encoded as the SAM {@code TYPE:VALUE} optional-field form
 * (e.g. {@code i:0}, {@code Z:group1}, {@code A:P}, {@code f:3.14}, {@code B:i,1,2,3}) so that
 * downstream consumers can read the SAM type and parse the value safely instead of receiving a
 * type-erased string.
 *
 * <h3>Column naming</h3>
 * Two interchangeable name sets are available, selected by the {@value #COLUMN_NAMES_OPTION}
 * read option:
 * <ul>
 *   <li>{@value #DESCRIPTIVE} (default) — verbose names ({@code readName}, {@code flags}, …).</li>
 *   <li>{@value #SAM} — canonical SAM-spec field names ({@code qname}, {@code flag}, {@code rname},
 *       {@code pos}, {@code mapq}, {@code cigar}, {@code rnext}, {@code pnext}, {@code tlen},
 *       {@code seq}, {@code qual}).</li>
 * </ul>
 * Both sets share identical column order, types, and nullability — only the alignment field
 * names differ. The {@code attributes} and {@code start0} extension columns have no SAM-spec
 * equivalent and keep their names in both.
 */
public class BamSchema {

    /** Read option selecting the column name set. */
    public static final String COLUMN_NAMES_OPTION = "columnNames";
    /** {@value #COLUMN_NAMES_OPTION} value for the verbose default names. */
    public static final String DESCRIPTIVE = "descriptive";
    /** {@value #COLUMN_NAMES_OPTION} value for canonical SAM-spec field names. */
    public static final String SAM = "sam";

    /** Schema with verbose, descriptive column names (the default). */
    public static final StructType SCHEMA = buildSchema(false);

    /** Schema with canonical SAM-spec field names ({@code qname}, {@code flag}, …). */
    public static final StructType SAM_SCHEMA = buildSchema(true);

    /** Name of the hidden, Databricks/Spark-compatible file metadata column. */
    public static final String METADATA_COLUMN_NAME = "_metadata";

    /**
     * Struct type of the {@value #METADATA_COLUMN_NAME} column. A subset of Spark's built-in
     * file-source metadata column, using identical field names and types so queries written
     * against {@code _metadata.file_path} (etc.) port over unchanged. Block-offset and row-index
     * fields are intentionally omitted: they have no stable meaning under genomic partitioning.
     */
    public static final StructType FILE_METADATA_TYPE = new StructType()
        .add("file_path",              DataTypes.StringType,    false)
        .add("file_name",              DataTypes.StringType,    false)
        .add("file_size",              DataTypes.LongType,      false)
        .add("file_modification_time", DataTypes.TimestampType, false);

    private static StructType buildSchema(boolean sam) {
        return new StructType()
            .add(sam ? "qname" : "readName",          DataTypes.StringType,  true)
            .add(sam ? "flag"  : "flags",             DataTypes.IntegerType, false)
            .add(sam ? "rname" : "referenceName",     DataTypes.StringType,  true)
            .add(sam ? "pos"   : "start",             DataTypes.LongType,    true) // 1-based (SAM spec)
            .add(sam ? "mapq"  : "mappingQuality",    DataTypes.IntegerType, true)
            .add("cigar",                             DataTypes.StringType,  true)
            .add(sam ? "rnext" : "mateReferenceName", DataTypes.StringType,  true)
            .add(sam ? "pnext" : "mateStart",         DataTypes.LongType,    true) // 1-based (SAM spec)
            .add(sam ? "tlen"  : "insertSize",        DataTypes.IntegerType, true)
            .add(sam ? "seq"   : "sequence",          DataTypes.StringType,  true)
            .add(sam ? "qual"  : "baseQualities",     DataTypes.StringType,  true)
            .add("attributes",                        DataTypes.createMapType(DataTypes.StringType,
                                                          DataTypes.StringType, true), true)
            .add("start0",                            DataTypes.LongType,    true); // 0-based (BED-compatible)
    }

    /** True when {@code value} selects the SAM-spec name set (case-insensitive). */
    public static boolean isSamColumnNames(String value) {
        return SAM.equalsIgnoreCase(value);
    }

    /** Returns the schema for the given {@value #COLUMN_NAMES_OPTION} value, defaulting to descriptive. */
    public static StructType forColumnNames(String value) {
        return isSamColumnNames(value) ? SAM_SCHEMA : SCHEMA;
    }

    /**
     * Resolves the schema from a (possibly case-varying) options/properties map.
     * Looks up {@value #COLUMN_NAMES_OPTION} case-insensitively and defaults to the descriptive schema.
     */
    public static StructType fromOptions(Map<String, String> options) {
        return forColumnNames(lookup(options));
    }

    /** True when the map selects the SAM-spec name set. */
    public static boolean isSamColumnNames(Map<String, String> options) {
        return isSamColumnNames(lookup(options));
    }

    private static String lookup(Map<String, String> options) {
        if (options == null) return null;
        for (Map.Entry<String, String> e : options.entrySet()) {
            if (COLUMN_NAMES_OPTION.equalsIgnoreCase(e.getKey())) return e.getValue();
        }
        return null;
    }

    private BamSchema() {}
}
