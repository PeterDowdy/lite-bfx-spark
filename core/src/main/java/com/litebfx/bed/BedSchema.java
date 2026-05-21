package com.litebfx.bed;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Coordinate note: {@code chromStart}, {@code chromEnd}, {@code thickStart}, and
 * {@code thickEnd} are <b>0-based half-open</b> intervals as defined by the BED
 * specification. This differs from BAM ({@code start} is 1-based), so take care
 * when joining BED and BAM data on genomic coordinates.
 */
public class BedSchema {

    public static final StructType SCHEMA = new StructType()
        .add("chrom",       DataTypes.StringType,  false)
        .add("chromStart",  DataTypes.LongType,    false) // 0-based (BED spec)
        .add("chromEnd",    DataTypes.LongType,    false) // 0-based exclusive (BED spec)
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
