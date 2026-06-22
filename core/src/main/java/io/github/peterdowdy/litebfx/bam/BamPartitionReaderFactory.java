package io.github.peterdowdy.litebfx.bam;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link BamPartitionReader} instances for each {@link BamInputPartition}.
 */
public class BamPartitionReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(BamPartitionReaderFactory.class);

    private final boolean includeAttributes;
    private final boolean includeFileMetadata;

    BamPartitionReaderFactory(boolean includeAttributes, boolean includeFileMetadata) {
        log.trace("BamPartitionReaderFactory(includeAttributes={}, includeFileMetadata={})",
                includeAttributes, includeFileMetadata);
        this.includeAttributes = includeAttributes;
        this.includeFileMetadata = includeFileMetadata;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new BamPartitionReader((BamInputPartition) partition, includeAttributes, includeFileMetadata);
    }
}
