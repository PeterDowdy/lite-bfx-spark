package com.litebfx;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Entry point for the {@code bam} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("bam")} resolves to this class.
 */
public class BamDataSource implements TableProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "bam";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return BamSchema.SCHEMA;
    }

    @Override
    public Table getTable(StructType schema,
                          Transform[] partitioning,
                          Map<String, String> properties) {
        return new BamTable(properties);
    }
}
