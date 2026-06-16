package io.github.peterdowdy.litebfx.fasta;

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
 * Entry point for the {@code fasta} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("fasta")} resolves to this class.
 *
 * <p>Supported options:
 * <ul>
 *   <li>{@code indexPath} — explicit path to a {@code .fai} index file</li>
 * </ul>
 */
public class FastaDataSource implements TableProvider, DataSourceRegister {

    private static final Logger log = LoggerFactory.getLogger(FastaDataSource.class);

    @Override
    public String shortName() {
        log.trace("shortName()");
        return "fasta";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        log.trace("inferSchema(options={})", options);
        return FastaSchema.SCHEMA;
    }

    @Override
    public Table getTable(StructType schema,
                          Transform[] partitioning,
                          Map<String, String> properties) {
        log.trace("getTable(schema={}, properties={})", schema, properties);
        return new FastaTable(properties);
    }
}
