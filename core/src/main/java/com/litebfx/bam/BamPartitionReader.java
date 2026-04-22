package com.litebfx.bam;

import com.litebfx.HadoopSeekableStream;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.StringLineReader;
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

    /** Minimum plausible BAM record body length in bytes (8 fixed int fields + 2-byte name ".\0"). */
    private static final int MIN_BAM_RECORD_BODY = 36;
    /** Maximum plausible BAM record body length used as validity upper bound (100 MB). */
    private static final int MAX_BAM_RECORD_BODY = 100_000_000;
    /** BGZF block magic bytes: gzip ID1/ID2, CM=deflate, FLG=extra-field. */
    private static final byte[] BGZF_MAGIC = {(byte) 0x1f, (byte) 0x8b, 0x08, 0x04};

    private boolean opened = false;
    private FSDataInputStream fsInputStream;
    private FSDataInputStream baiInputStream;
    private SamReader samReader;
    private SAMRecordIterator iterator;
    private SAMRecord current;

    // BGZF split mode — non-null only when startVirtualOffset > 0 or endVirtualOffset != MAX_VALUE
    private BlockCompressedInputStream bcis;
    private BAMRecordCodec bamRecordCodec;
    private FSDataInputStream bgzfFsInputStream;

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

        // BGZF split mode: use BAMRecordCodec with block-address stop condition.
        if (bcis != null) {
            if (bamRecordCodec == null) return false; // empty partition (no clean record start found)
            long endByte = partition.getEndByte();
            if (endByte != Long.MAX_VALUE) {
                long blockAddr = BlockCompressedFilePointerUtil.getBlockAddress(bcis.getFilePointer());
                if (blockAddr >= endByte) {
                    log.trace("next() BGZF split: reached end boundary blockAddr={} endByte={}", blockAddr, endByte);
                    return false;
                }
            }
            SAMRecord r = (SAMRecord) bamRecordCodec.decode();
            if (r == null) return false;
            current = r;
            log.trace("next() BGZF split -> readName={}", current.getReadName());
            return true;
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
            if (bcis != null) {
                bcis.close(); // closes its wrapped HadoopSeekableStream → bgzfFsInputStream
                bgzfFsInputStream = null; // prevent double-close in finally
            }
            if (iterator != null) iterator.close();
            if (samReader != null) samReader.close();
        } finally {
            // bgzfFsInputStream is non-null only if bcis.close() threw above
            if (bgzfFsInputStream != null) bgzfFsInputStream.close();
            if (baiInputStream != null) baiInputStream.close();
            if (fsInputStream != null) fsInputStream.close();
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void open() throws IOException {
        log.trace("open() path={}", partition.getPath());

        // BGZF split mode: triggered when the planner set byte-range boundaries.
        boolean isBgzfSplitMode = partition.getEndByte() != Long.MAX_VALUE
                || partition.getStartByte() > 0;
        if (isBgzfSplitMode) {
            log.trace("open() BGZF split mode startByte={} endByte={}",
                    partition.getStartByte(), partition.getEndByte());
            openBgzfSplit();
            return;
        }

        Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(partition.getPath());
        FileSystem fs = hadoopPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(hadoopPath).getLen();
        log.trace("open() fileLength={}", fileLength);

        fsInputStream = fs.open(hadoopPath);
        boolean success = false;
        try {
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
                // Guard against a pushed reference name that does not exist in this file's header
                // (e.g. a filter like referenceName = 'NONEXISTENT_CHROM'). htsjdk throws
                // IllegalArgumentException if the reference index is -1, so return empty iterator.
                int refIdx = samReader.getFileHeader().getSequenceIndex(partition.getQuerySequence());
                if (refIdx < 0) {
                    log.trace("open() reference '{}' not in header — empty partition", partition.getQuerySequence());
                    iterator = samReader.query(new QueryInterval[0], false);
                } else {
                    iterator = samReader.query(
                            partition.getQuerySequence(),
                            partition.getQueryStart(),
                            partition.getQueryEnd(),
                            false);
                }

            } else {
                // Full-file scan (SAM files, BAM without BAI, or useIndex=false).
                log.trace("open() full-file scan");
                samReader = factory.open(SamInputResource.of(seekable));
                iterator = samReader.iterator();
            }
            log.trace("open() SamReader opened successfully");
            success = true;
        } finally {
            if (!success) {
                if (baiInputStream != null) try { baiInputStream.close(); } catch (IOException e) {
                    log.debug("suppressed exception closing BAI stream", e);
                }
                try { fsInputStream.close(); } catch (IOException e) {
                    log.debug("suppressed exception closing BAM stream", e);
                }
            }
        }
    }

    /**
     * Opens the partition in BGZF split mode.
     *
     * <p>Reads the BAM header from byte 0 using a temporary stream, then opens a second
     * stream positioned at the first BGZF block in this partition's byte range whose
     * decompressed content begins at a clean BAM record boundary. Partitions where no
     * such block exists are marked empty ({@link #bamRecordCodec} remains null).
     *
     * <p>For cross-block records (e.g. PacBio/Nanopore reads > ~65 KB uncompressed),
     * the record is owned entirely by the partition that holds the block where it starts.
     * Subsequent partitions skip the tail block(s) by rejecting blocks whose first 4
     * decompressed bytes do not form a plausible BAM record-body length.
     */
    private void openBgzfSplit() throws IOException {
        Configuration conf = partition.getHadoopConf();
        Path bamPath = new Path(partition.getPath());
        FileSystem fs = bamPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(bamPath).getLen();

        // ── Step A: parse BAM header from byte 0 to get SAMFileHeader + firstDataVFO ──
        SAMFileHeader header;
        long firstDataVFO;
        FSDataInputStream hdrStream = fs.open(bamPath);
        try {
            BlockCompressedInputStream hdrBcis = new BlockCompressedInputStream(
                    new HadoopSeekableStream(hdrStream, fileLength, partition.getPath()));
            BinaryCodec bc = new BinaryCodec(hdrBcis);

            byte[] magic = new byte[4];
            if (hdrBcis.read(magic, 0, 4) < 4) throw new IOException("Truncated BAM magic");

            int lText = bc.readInt();
            byte[] headerText = new byte[lText];
            for (int off = 0; off < lText; ) {
                int r = hdrBcis.read(headerText, off, lText - off);
                if (r < 0) throw new IOException("Truncated BAM header text");
                off += r;
            }

            int nRef = bc.readInt();
            for (int i = 0; i < nRef; i++) {
                int lName = bc.readInt();
                for (long rem = lName + 4; rem > 0; ) {
                    long sk = hdrBcis.skip(rem);
                    if (sk <= 0) throw new IOException("Truncated BAM reference section");
                    rem -= sk;
                }
            }
            firstDataVFO = hdrBcis.getFilePointer();
            log.trace("openBgzfSplit() firstDataVFO={}", firstDataVFO);

            SAMTextHeaderCodec headerCodec = new SAMTextHeaderCodec();
            headerCodec.setValidationStringency(ValidationStringency.LENIENT);
            header = headerCodec.decode(
                    new StringLineReader(new String(headerText, java.nio.charset.StandardCharsets.US_ASCII)),
                    partition.getPath());
            hdrBcis.close();
        } finally {
            hdrStream.close();
        }

        // ── Step B: open data stream, seek to first clean record start in our range ──
        bgzfFsInputStream = fs.open(bamPath);
        boolean success = false;
        try {
            bcis = new BlockCompressedInputStream(
                    new HadoopSeekableStream(bgzfFsInputStream, fileLength, partition.getPath()));

            long startVFO;
            if (partition.getStartByte() == 0) {
                // Partition 0: the header parser already found the first data record.
                startVFO = firstDataVFO;
                log.trace("openBgzfSplit() partition 0: startVFO={}", startVFO);
            } else {
                long cleanBlockByte = findCleanRecordStart(
                        bgzfFsInputStream, bcis,
                        partition.getStartByte(),
                        partition.getEndByte());
                if (cleanBlockByte < 0) {
                    log.trace("openBgzfSplit() no clean record start found — empty partition");
                    bamRecordCodec = null;
                    success = true;
                    return;
                }
                startVFO = BlockCompressedFilePointerUtil.makeFilePointer(cleanBlockByte);
                log.trace("openBgzfSplit() clean block at byte={} vfo={}", cleanBlockByte, startVFO);
            }

            bcis.seek(startVFO);
            bamRecordCodec = new BAMRecordCodec(header);
            bamRecordCodec.setInputStream(bcis, partition.getPath());
            success = true;
        } finally {
            if (!success) {
                if (bcis != null) {
                    try { bcis.close(); } catch (IOException e) { log.debug("suppressed bcis close", e); }
                    // bcis.close() closed bgzfFsInputStream via HadoopSeekableStream
                } else {
                    bgzfFsInputStream.close();
                }
            }
        }
    }

    /**
     * Scans BGZF blocks starting at {@code searchFrom} for the first block whose
     * decompressed content begins at a clean BAM record boundary (first 4 bytes form a
     * plausible record-body length). Blocks that start mid-record are skipped.
     *
     * @return raw file offset of the qualifying block, or -1 if none found in range
     */
    private static long findCleanRecordStart(FSDataInputStream rawStream,
                                              BlockCompressedInputStream bcis,
                                              long searchFrom,
                                              long endByte) throws IOException {
        long candidate = findNextBgzfBlockStart(rawStream, searchFrom);
        while (candidate >= 0) {
            if (endByte != Long.MAX_VALUE && candidate >= endByte) return -1L;

            bcis.seek(BlockCompressedFilePointerUtil.makeFilePointer(candidate));
            byte[] probe = new byte[4];
            int n = bcis.read(probe, 0, 4);
            if (n == 4) {
                int bodyLen = ((probe[3] & 0xff) << 24) | ((probe[2] & 0xff) << 16)
                            | ((probe[1] & 0xff) << 8)  |  (probe[0] & 0xff);
                if (bodyLen >= MIN_BAM_RECORD_BODY && bodyLen <= MAX_BAM_RECORD_BODY) {
                    return candidate;
                }
            }
            // Block starts mid-record; advance past it to try the next one.
            candidate = findNextBgzfBlockStart(rawStream, candidate + 1);
        }
        return -1L;
    }

    /**
     * Scans raw bytes starting at {@code startByte} for the first occurrence of the
     * BGZF 4-byte magic ({@code 0x1f 0x8b 0x08 0x04}).
     *
     * <p>Reads one buffer of {@code MAX_COMPRESSED_BLOCK_SIZE + 4} bytes — sufficient
     * to always contain the start of the next contiguous BGZF block.
     *
     * @return file offset of the first magic match, or -1 if not found
     */
    private static long findNextBgzfBlockStart(FSDataInputStream stream, long startByte)
            throws IOException {
        stream.seek(startByte);
        int bufSize = BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE + 4;
        byte[] buf = new byte[bufSize];
        int total = 0;
        while (total < bufSize) {
            int n = stream.read(buf, total, bufSize - total);
            if (n < 0) break;
            total += n;
        }
        for (int i = 0; i <= total - 4; i++) {
            if (buf[i] == BGZF_MAGIC[0] && buf[i + 1] == BGZF_MAGIC[1]
                    && buf[i + 2] == BGZF_MAGIC[2] && buf[i + 3] == BGZF_MAGIC[3]) {
                return startByte + i;
            }
        }
        return -1L;
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
