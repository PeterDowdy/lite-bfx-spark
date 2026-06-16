package io.github.peterdowdy.litebfx.vcf;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

/** Spark schema for VCF/BCF: column names, types, and nullability for all standard fields. */
public class VcfSchema {

    public static final StructType SCHEMA = new StructType()
        .add("chrom",     DataTypes.StringType,                              false)
        .add("pos",       DataTypes.IntegerType,                             false)
        .add("id",        DataTypes.StringType,                              true)
        .add("ref",       DataTypes.StringType,                              false)
        .add("alt",       DataTypes.createArrayType(DataTypes.StringType, true), true)
        .add("qual",      DataTypes.DoubleType,  true)
        .add("filter",    DataTypes.StringType,  true)
        .add("info",      new MapType(DataTypes.StringType, DataTypes.StringType, true), false)
        .add("format",    DataTypes.StringType,  true)
        .add("genotypes", new MapType(DataTypes.StringType, DataTypes.StringType, true), true);

    private VcfSchema() {}
}
