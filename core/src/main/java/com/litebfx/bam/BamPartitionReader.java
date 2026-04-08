package com.litebfx.bam;

import com.litebfx.HadoopSeekableStream;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.ref.ReferenceSource;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Reads {@link SAMRecord}s from a single BAM/SAM/CRAM partition and converts them
 * to Spark {@link InternalRow}s matching {@link BamSchema#SCHEMA}.
 *
 * <h3>Partition modes</h3>
 * The mode is determined by fields on {@link BamInputPartition} (checked in order):
 * <ol>
 *   <li><b>Unmapped</b>: {@code queryUnmapped=true} → {@code samReader.queryUnmapped()}.
 *       Reads only unplaced, unmapped reads at the tail of the file.  Requires BAI.</li>
 *   <li><b>Per-reference VFO</b>: {@code querySequences != null} → builds a
 *       {@link QueryInterval}{@code []} and calls {@code samReader.query(intervals, false)}.
 *       htsjdk uses the BAI's virtual file offsets (VFOs) internally to seek directly
 *       to each reference's data, avoiding a full-file scan.</li>
 *   <li><b>Region push-down</b>: {@code querySequence != null} + {@code indexPath != null} →
 *       {@code samReader.query(ref, start, end, false)}.</li>
 *   <li><b>Full scan</b>: fallback → {@code samReader.iterator()}.</li>
 * </ol>
 */
public class BamPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(BamPartitionReader.class);

    private final BamInputPartition partition;
    private final boolean includeAttributes;

    private boolean opened = false;
    private FSDataInputStream fsInputStream;
    private FSDataInputStream baiInputStream;
    private SamReader samReader;
    private SAMRecordIterator iterator;
    private SAMRecord current;

    public BamPartitionReader(BamInputPartition partition, boolean includeAttributes) {
        log.trace("BamPartitionReader(path={}, includeAttributes={})", partition.getPath(), includeAttributes);
        this.partition = partition;
        this.includeAttributes = includeAttributes;
    }

    // -------------------------------------------------------------------------
    // PartitionReader contract
    // -------------------------------------------------------------------------

    @Override
    public boolean next() throws IOException {
        log.trace("next()");
        if (!opened) {
            open();
            opened = true;
        }
        if (!iterator.hasNext()) return false;
        current = iterator.next();
        log.trace("next() -> read record readName={}", current.getReadName());
        return true;
    }

    @Override
    public InternalRow get() {
        log.trace("get() readName={}", current.getReadName());
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
        log.trace("close() path={}", partition.getPath());
        try {
            if (iterator != null) iterator.close();
            if (samReader != null) samReader.close();
        } finally {
            if (baiInputStream != null) baiInputStream.close();
            if (fsInputStream != null) fsInputStream.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        log.trace("open() path={}", partition.getPath());

        Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(partition.getPath());
        FileSystem fs = hadoopPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(hadoopPath).getLen();
        log.trace("open() fileLength={}", fileLength);

        fsInputStream = fs.open(hadoopPath);
        HadoopSeekableStream seekable = new HadoopSeekableStream(
            fsInputStream, fileLength, partition.getPath());

        SamReaderFactory factory = SamReaderFactory.makeDefault()
            .validationStringency(ValidationStringency.LENIENT);

        if (partition.isCram()) {
            String referenceFile = partition.getReferenceFile();
            if (referenceFile != null && !"none".equals(partition.getReferenceMode())) {
                log.trace("open() CRAM with referenceFile={}", referenceFile);
                factory = factory.referenceSource(new ReferenceSource(new java.io.File(referenceFile)));
            } else {
                log.trace("open() CRAM with no external reference");
                factory = factory.referenceSource(new ReferenceSource((java.io.File) null));
            }
        }

        if (partition.isQueryUnmapped()) {
            // Unplaced unmapped reads — BAI required to locate them in the file.
            log.trace("open() querying unmapped reads");
            if (partition.getIndexPath() != null) {
                HadoopSeekableStream baiSeekable = openBaiStream(conf);
                samReader = factory.open(SamInputResource.of(seekable).index(baiSeekable));
            } else {
                samReader = factory.open(SamInputResource.of(seekable));
            }
            iterator = samReader.queryUnmapped();

        } else if (partition.getQuerySequences() != null) {
            // Per-reference VFO partitioning: htsjdk uses BAI VFO chunks internally.
            String[] seqs = partition.getQuerySequences();
            log.trace("open() per-reference VFO query sequences={}", (Object) seqs);
            if (partition.getIndexPath() != null) {
                HadoopSeekableStream baiSeekable = openBaiStream(conf);
                samReader = factory.open(SamInputResource.of(seekable).index(baiSeekable));
                if (seqs.length == 1) {
                    iterator = samReader.query(seqs[0], 1, Integer.MAX_VALUE, false);
                    log.trace("open() single-ref query sequence={}", seqs[0]);
                } else {
                    SAMFileHeader header = samReader.getFileHeader();
                    QueryInterval[] intervals = new QueryInterval[seqs.length];
                    for (int i = 0; i < seqs.length; i++) {
                        int refIdx = header.getSequenceIndex(seqs[i]);
                        intervals[i] = new QueryInterval(refIdx, 1, 0); // 0 = until end of reference
                    }
                    intervals = QueryInterval.optimizeIntervals(intervals);
                    iterator = samReader.query(intervals, false);
                    log.trace("open() multi-ref query intervals={}", seqs.length);
                }
            } else {
                // No index available; fall back to full-file scan.
                log.trace("open() querySequences set but no index; falling back to full-file scan");
                samReader = factory.open(SamInputResource.of(seekable));
                iterator = samReader.iterator();
            }

        } else if (partition.getQuerySequence() != null && partition.getIndexPath() != null) {
            // BAI/CRAI-guided region push-down query.
            log.trace("open() BAI-guided query sequence={} start={} end={}",
                    partition.getQuerySequence(), partition.getQueryStart(), partition.getQueryEnd());
            HadoopSeekableStream baiSeekable = openBaiStream(conf);
            samReader = factory.open(SamInputResource.of(seekable).index(baiSeekable));
            iterator = samReader.query(
                    partition.getQuerySequence(),
                    partition.getQueryStart(),
                    partition.getQueryEnd(),
                    false);

        } else {
            // Full-file scan (SAM files, BAM without BAI, or useIndex=false).
            log.trace("open() full-file scan");
            samReader = factory.open(SamInputResource.of(seekable));
            iterator = samReader.iterator();
        }
        log.trace("open() SamReader opened successfully");
    }

    /**
     * Opens the BAI/CRAI as a {@link HadoopSeekableStream}.
     * The returned stream is backed by {@link #baiInputStream} which is closed in {@link #close()}.
     */
    private HadoopSeekableStream openBaiStream(Configuration conf) throws IOException {
        Path baiHadoopPath = new Path(partition.getIndexPath());
        FileSystem baiFs = baiHadoopPath.getFileSystem(conf);
        long baiLen = baiFs.getFileStatus(baiHadoopPath).getLen();
        baiInputStream = baiFs.open(baiHadoopPath);
        return new HadoopSeekableStream(baiInputStream, baiLen, partition.getIndexPath());
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
