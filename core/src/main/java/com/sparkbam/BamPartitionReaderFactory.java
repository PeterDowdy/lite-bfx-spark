package com.litebfx;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * Creates {@link BamPartitionReader} instances for each {@link BamInputPartition}.
 */
public class BamPartitionReaderFactory implements PartitionReaderFactory {

    private final boolean includeAttributes;

    BamPartitionReaderFactory(boolean includeAttributes) {
        this.includeAttributes = includeAttributes;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new BamPartitionReader((BamInputPartition) partition, includeAttributes);
    }
}
