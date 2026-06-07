package com.litebfx.bam;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark schema for BAM/SAM/CRAM: column names, types, and nullability for all alignment fields.
 *
 * <p>Coordinate note: {@code start} and {@code mateStart} are <b>1-based</b> positions
 * as defined by the SAM specification. This differs from BED ({@code chromStart} is
 * 0-based), so take care when joining BAM and BED data on genomic coordinates.
 */
public class BamSchema {

    public static final StructType SCHEMA = new StructType()
        .add("readName",          DataTypes.StringType,                              true)
        .add("flags",             DataTypes.IntegerType,                             false)
        .add("referenceName",     DataTypes.StringType,                              true)
        .add("start",             DataTypes.LongType,                             true) // 1-based (SAM spec)
        .add("mappingQuality",    DataTypes.IntegerType,                             true)
        .add("cigar",             DataTypes.StringType,                              true)
        .add("mateReferenceName", DataTypes.StringType,                              true)
        .add("mateStart",         DataTypes.LongType,                             true) // 1-based (SAM spec)
        .add("insertSize",        DataTypes.IntegerType,                             true)
        .add("sequence",          DataTypes.StringType,                              true)
        .add("baseQualities",     DataTypes.StringType,                              true)
        .add("attributes",        DataTypes.createMapType(DataTypes.StringType,
                                                          DataTypes.StringType, true), true)
        .add("start0",            DataTypes.LongType,                             true); // 0-based (BED-compatible)

    private BamSchema() {}
}
