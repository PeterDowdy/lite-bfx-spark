package com.litebfx;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.List;

/**
 * Reads {@link SAMRecord}s from a single BAM/SAM partition and converts them
 * to Spark {@link InternalRow}s matching {@link BamSchema#SCHEMA}.
 *
 * <h3>Partition bounds</h3>
 * The reader always performs a full-file scan via {@code samReader.iterator()}.
 * Region filtering is handled by Spark's post-scan filter pass (all filters are
 * returned as unhandled from {@link BamScanBuilder#pushFilters}).
 *
 * <p>VFO-based seeking (for BAI-split partitions with {@code startVirtualOffset > 0})
 * is deferred — calling such a partition throws {@link UnsupportedOperationException}.
 * End-boundary enforcement via VFO is deferred for the same reason.
 */
public class BamPartitionReader implements PartitionReader<InternalRow> {

    private final BamInputPartition partition;
    private final boolean includeAttributes;

    private boolean opened = false;
    private FSDataInputStream fsInputStream;
    private SamReader samReader;
    private SAMRecordIterator iterator;
    private SAMRecord current;

    public BamPartitionReader(BamInputPartition partition, boolean includeAttributes) {
        this.partition = partition;
        this.includeAttributes = includeAttributes;
    }

    // -------------------------------------------------------------------------
    // PartitionReader contract
    // -------------------------------------------------------------------------

    @Override
    public boolean next() throws IOException {
        if (!opened) {
            open();
            opened = true;
        }
        if (!iterator.hasNext()) return false;
        current = iterator.next();
        return true;
    }

    @Override
    public InternalRow get() {
        Object[] values = new Object[12];
        values[0]  = toUTF8(current.getReadName());
        values[1]  = current.getFlags();
        values[2]  = toUTF8(current.getContig());
        values[3]  = current.getAlignmentStart();
        values[4]  = current.getMappingQuality();
        values[5]  = toUTF8(current.getCigarString());
        values[6]  = toUTF8(mateContig(current));
        values[7]  = current.getMateAlignmentStart();
        values[8]  = current.getInferredInsertSize();
        values[9]  = toUTF8(current.getReadString());
        values[10] = toUTF8(current.getBaseQualityString());
        values[11] = includeAttributes ? buildAttributesMap(current) : null;
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        try {
            if (iterator != null) iterator.close();
            if (samReader != null) samReader.close();
        } finally {
            if (fsInputStream != null) fsInputStream.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        long startVFO = partition.getStartVirtualOffset();
        if (startVFO > 0) {
            throw new UnsupportedOperationException(
                "VFO-based seeking not yet implemented (startVirtualOffset=" + startVFO + "). " +
                "BamScan will produce only startVirtualOffset=0 partitions until VFO splitting is wired up.");
        }

        Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(partition.getPath());
        FileSystem fs = hadoopPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(hadoopPath).getLen();

        fsInputStream = fs.open(hadoopPath);
        HadoopSeekableStream seekable = new HadoopSeekableStream(
            fsInputStream, fileLength, partition.getPath());

        samReader = SamReaderFactory.makeDefault()
            .validationStringency(ValidationStringency.LENIENT)
            .open(SamInputResource.of(seekable));

        iterator = samReader.iterator();
    }

    /** Returns a UTF8String for non-null input, or null for null (maps to Spark null). */
    private static UTF8String toUTF8(String s) {
        return s == null ? null : UTF8String.fromString(s);
    }

    /**
     * Returns the mate's reference sequence name, or null when the mate is
     * unmapped ({@link SAMRecord#NO_ALIGNMENT_REFERENCE_INDEX}).
     * Uses index-based lookup for compatibility across htsjdk versions.
     */
    private static String mateContig(SAMRecord record) {
        int idx = record.getMateReferenceIndex();
        if (idx == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) return null;
        SAMSequenceRecord seq = record.getHeader().getSequence(idx);
        return seq == null ? null : seq.getSequenceName();
    }

    private static ArrayBasedMapData buildAttributesMap(SAMRecord record) {
        List<SAMRecord.SAMTagAndValue> attrs = record.getAttributes();
        int n = attrs.size();
        UTF8String[] keys   = new UTF8String[n];
        UTF8String[] values = new UTF8String[n];
        for (int i = 0; i < n; i++) {
            SAMRecord.SAMTagAndValue tv = attrs.get(i);
            keys[i]   = UTF8String.fromString(tv.tag);
            values[i] = UTF8String.fromString(String.valueOf(tv.value));
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
    }
}
