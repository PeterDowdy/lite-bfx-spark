package com.litebfx.fasta;

import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

/**
 * Reads one or all contigs from a FASTA file and converts them to Spark
 * {@link InternalRow}s matching {@link FastaSchema#SCHEMA}.
 *
 * <h3>Indexed (per-contig) mode</h3>
 * When {@link FastaInputPartition#getContigName()} is non-null, the reader
 * calls {@code getSequence(contigName)} on an indexed
 * {@code ReferenceSequenceFile}, producing exactly one row.
 *
 * <h3>Full-scan mode</h3>
 * When {@code contigName} is null, the reader iterates all sequences via
 * {@code nextSequence()}, producing one row per contig.
 */
public class FastaPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(FastaPartitionReader.class);

    private final FastaInputPartition partition;

    private ReferenceSequenceFile refFile;
    private ReferenceSequence current;
    /** For indexed (per-contig) mode: true once the single record has been returned. */
    private boolean done = false;

    public FastaPartitionReader(FastaInputPartition partition) {
        log.trace("FastaPartitionReader(path={}, contigName={})",
                partition.getPath(), partition.getContigName());
        this.partition = partition;
    }

    @Override
    public boolean next() throws IOException {
        if (refFile == null) {
            open();
        }

        if (partition.getContigName() != null) {
            // Indexed mode: one record per partition.
            if (done) return false;
            current = refFile.getSequence(partition.getContigName());
            done = true;
            log.trace("next() indexed contig={} len={}", current.getName(), current.length());
            return current != null;
        } else {
            // Full-scan mode: iterate all sequences.
            current = refFile.nextSequence();
            if (current == null) return false;
            log.trace("next() full-scan contig={} len={}", current.getName(), current.length());
            return true;
        }
    }

    @Override
    public InternalRow get() {
        log.trace("get() contig={}", current.getName());
        byte[] bases = current.getBases();
        Object[] values = new Object[3];
        values[0] = UTF8String.fromString(current.getName());
        values[1] = UTF8String.fromBytes(bases);
        values[2] = (long) bases.length;
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        log.trace("close() path={}", partition.getPath());
        if (refFile != null) {
            refFile.close();
        }
    }

    private void open() throws IOException {
        log.trace("open() path={}", partition.getPath());
        java.nio.file.Path nioPath = toNioPath(partition.getPath());
        refFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(nioPath);
        log.trace("open() ReferenceSequenceFile opened, isIndexed={}", refFile.isIndexed());
    }

    private static java.nio.file.Path toNioPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            if (uri.getScheme() == null) {
                return Paths.get(pathStr);
            }
            return Paths.get(uri);
        } catch (Exception e) {
            // Fallback: treat as a plain local path
            return Paths.get(pathStr);
        }
    }
}
