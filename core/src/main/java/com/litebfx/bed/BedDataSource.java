package com.litebfx.bed;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Entry point for the {@code bed} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("bed")} resolves to this class.
 */
public class BedDataSource implements TableProvider, DataSourceRegister {

    private static final Logger log = LoggerFactory.getLogger(BedDataSource.class);

    @Override
    public String shortName() {
        return "bed";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return BedSchema.SCHEMA;
    }

    @Override
    public Table getTable(StructType schema,
                          Transform[] partitioning,
                          Map<String, String> properties) {
        log.trace("getTable(properties={})", properties);
        return new BedTable(properties);
    }
}
