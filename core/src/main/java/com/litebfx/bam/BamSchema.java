package com.litebfx.bam;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class BamSchema {

    public static final StructType SCHEMA = new StructType()
        .add("readName",          DataTypes.StringType,                              true)
        .add("flags",             DataTypes.IntegerType,                             false)
        .add("referenceName",     DataTypes.StringType,                              true)
        .add("start",             DataTypes.IntegerType,                             true)
        .add("mappingQuality",    DataTypes.IntegerType,                             true)
        .add("cigar",             DataTypes.StringType,                              true)
        .add("mateReferenceName", DataTypes.StringType,                              true)
        .add("mateStart",         DataTypes.IntegerType,                             true)
        .add("insertSize",        DataTypes.IntegerType,                             true)
        .add("sequence",          DataTypes.StringType,                              true)
        .add("baseQualities",     DataTypes.StringType,                              true)
        .add("attributes",        DataTypes.createMapType(DataTypes.StringType,
                                                          DataTypes.StringType, true), true);

    private BamSchema() {}
}
