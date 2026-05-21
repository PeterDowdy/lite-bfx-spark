package com.litebfx.fastq;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates {@link FastqPartitionReader} instances for each {@link FastqInputPartition}. */
public class FastqPartitionReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(FastqPartitionReaderFactory.class);

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new FastqPartitionReader((FastqInputPartition) partition);
    }
}
