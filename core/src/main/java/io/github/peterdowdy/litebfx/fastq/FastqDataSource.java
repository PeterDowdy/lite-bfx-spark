package io.github.peterdowdy.litebfx.fastq;

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
 * Entry point for the {@code fastq} DataSource V2.
 *
 * <p>Registered via the {@code META-INF/services} service-loader mechanism so
 * that {@code spark.read.format("fastq")} resolves to this class.
 *
 * <p>Supported options:
 * <ul>
 *   <li>{@code numPartitions} — maximum partitions for uncompressed files (default 200)</li>
 * </ul>
 *
 * <p>Gzipped files ({@code .fastq.gz} / {@code .fq.gz}) always produce a single
 * partition regardless of {@code numPartitions}.
 */
public class FastqDataSource implements TableProvider, DataSourceRegister {

    private static final Logger log = LoggerFactory.getLogger(FastqDataSource.class);

    @Override
    public String shortName() {
        log.trace("shortName()");
        return "fastq";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        log.trace("inferSchema(options={})", options);
        return FastqSchema.SCHEMA;
    }

    @Override
    public Table getTable(StructType schema,
                          Transform[] partitioning,
                          Map<String, String> properties) {
        log.trace("getTable(schema={}, properties={})", schema, properties);
        return new FastqTable(properties);
    }
}
