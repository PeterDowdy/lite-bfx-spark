package io.github.peterdowdy.litebfx.fasta;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates {@link FastaPartitionReader} instances. */
public class FastaPartitionReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(FastaPartitionReaderFactory.class);

    private final boolean includeFileMetadata;

    FastaPartitionReaderFactory(boolean includeFileMetadata) {
        this.includeFileMetadata = includeFileMetadata;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        log.trace("createReader(partition={})", partition);
        return new FastaPartitionReader((FastaInputPartition) partition, includeFileMetadata);
    }
}
