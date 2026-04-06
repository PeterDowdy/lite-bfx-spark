package com.litebfx.bed;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class BedSchema {

    public static final StructType SCHEMA = new StructType()
        .add("chrom",       DataTypes.StringType,  false)
        .add("chromStart",  DataTypes.LongType,    false)
        .add("chromEnd",    DataTypes.LongType,    false)
        .add("name",        DataTypes.StringType,  true)
        .add("score",       DataTypes.IntegerType, true)
        .add("strand",      DataTypes.StringType,  true)
        .add("thickStart",  DataTypes.LongType,    true)
        .add("thickEnd",    DataTypes.LongType,    true)
        .add("itemRgb",     DataTypes.StringType,  true)
        .add("blockCount",  DataTypes.IntegerType, true)
        .add("blockSizes",  DataTypes.StringType,  true)
        .add("blockStarts", DataTypes.StringType,  true);

    private BedSchema() {}
}
