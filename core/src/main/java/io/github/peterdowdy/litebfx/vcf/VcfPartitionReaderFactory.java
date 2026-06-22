package io.github.peterdowdy.litebfx.vcf;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link VcfPartitionReader} instances for each {@link VcfInputPartition}.
 */
public class VcfPartitionReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(VcfPartitionReaderFactory.class);

    private final boolean includeInfo;
    private final boolean includeGenotypes;
    private final boolean includeFileMetadata;

    VcfPartitionReaderFactory(boolean includeInfo, boolean includeGenotypes, boolean includeFileMetadata) {
        this.includeInfo         = includeInfo;
        this.includeGenotypes    = includeGenotypes;
        this.includeFileMetadata = includeFileMetadata;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new VcfPartitionReader((VcfInputPartition) partition, includeInfo, includeGenotypes,
                includeFileMetadata);
    }
}
