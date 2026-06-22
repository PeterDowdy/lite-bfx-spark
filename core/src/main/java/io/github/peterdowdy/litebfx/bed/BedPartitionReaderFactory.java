package io.github.peterdowdy.litebfx.bed;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link BedPartitionReader} instances for each {@link BedInputPartition}.
 */
public class BedPartitionReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(BedPartitionReaderFactory.class);

    private final boolean includeFileMetadata;

    BedPartitionReaderFactory(boolean includeFileMetadata) {
        this.includeFileMetadata = includeFileMetadata;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new BedPartitionReader((BedInputPartition) partition, includeFileMetadata);
    }
}
