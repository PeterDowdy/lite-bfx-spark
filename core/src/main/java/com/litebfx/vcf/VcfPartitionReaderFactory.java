package com.litebfx.vcf;

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

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new VcfPartitionReader((VcfInputPartition) partition);
    }
}
