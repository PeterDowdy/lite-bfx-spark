package io.github.peterdowdy.litebfx.bam;

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
 * Entry point for the {@code cram} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("cram")} resolves to this class.
 *
 * <p>Supported options:
 * <ul>
 *   <li>{@code referenceFile} — path to a FASTA reference file (required for most CRAM files)</li>
 *   <li>{@code referenceMode} — {@code "file"} (default), {@code "none"} (no external reference)</li>
 *   <li>{@code indexPath} — explicit path to the CRAI index file</li>
 *   <li>{@code indexDir} — directory to search for a co-located CRAI index</li>
 *   <li>{@code useIndex} — {@code "true"} (default) / {@code "false"}</li>
 * </ul>
 */
public class CramDataSource implements TableProvider, DataSourceRegister {

    private static final Logger log = LoggerFactory.getLogger(CramDataSource.class);

    @Override
    public String shortName() {
        log.trace("shortName()");
        return "cram";
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
        log.trace("getTable(schema={}, properties={})", schema, properties);
        return new BamTable(properties, true);
    }
}
