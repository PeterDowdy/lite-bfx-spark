package com.litebfx.bam;

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
 * Entry point for the {@code bam} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("bam")} resolves to this class.
 */
public class BamDataSource implements TableProvider, DataSourceRegister {

    private static final Logger log = LoggerFactory.getLogger(BamDataSource.class);

    @Override
    public String shortName() {
        log.trace("shortName()");
        return "bam";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        log.trace("inferSchema(options={})", options);
        return BamSchema.SCHEMA;
    }

    @Override
    public Table getTable(StructType schema,
                          Transform[] partitioning,
                          Map<String, String> properties) {
        log.trace("getTable(schema={}, partitioning={}, properties={})", schema, partitioning, properties);
        return new BamTable(properties);
    }
}
