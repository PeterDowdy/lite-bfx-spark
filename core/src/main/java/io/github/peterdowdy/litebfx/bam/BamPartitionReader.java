package io.github.peterdowdy.litebfx.bam;

import io.github.peterdowdy.litebfx.HadoopSeekableStream;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMLineParser;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.TextTagCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.CRAMIterator;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.StringLineReader;
import io.github.peterdowdy.litebfx.FileMetadata;
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

import java.io.BufferedInputStream;
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
    private final boolean includeFileMetadata;

    /**
     * htsjdk codec used to render each tag value as its SAM {@code TYPE:VALUE} text form.
     * Stateful and single-threaded, so it is owned per reader (each reader is driven by one task thread).
     */
    private final TextTagCodec tagCodec = new TextTagCodec();

    /** Cached {@code _metadata} struct value (file_path, …); computed once in {@link #open()}. */
    private InternalRow fileMetadataRow;

    /** Minimum plausible BAM record body length in bytes (8 fixed int fields + 2-byte name ".\0"). */
    private static final int MIN_BAM_RECORD_BODY = 36;
    /** Maximum plausible BAM record body length used as validity upper bound (100 MB). */
    private static final int MAX_BAM_RECORD_BODY = 100_000_000;
    /** BGZF block magic bytes: gzip ID1/ID2, CM=deflate, FLG=extra-field. */
    private static final byte[] BGZF_MAGIC = {(byte) 0x1f, (byte) 0x8b, 0x08, 0x04};

    private boolean opened = false;
    private long rowsRead = 0;
    private FSDataInputStream fsInputStream;
    private FSDataInputStream baiInputStream;
    private SamReader samReader;
    private SAMRecordIterator iterator;
    private SAMRecord current;

    // BGZF split mode — non-null only when startVirtualOffset > 0 or endVirtualOffset != MAX_VALUE
    private BlockCompressedInputStream bcis;
    private BAMRecordCodec bamRecordCodec;
    private FSDataInputStream bgzfFsInputStream;

    /** Sentinel: BGZF split with no reference filter (plain unindexed split). */
    private static final int NO_REF_FILTER = Integer.MIN_VALUE;
    /**
     * Reference index this BGZF-split partition is restricted to (hybrid indexed split), or
     * {@link #NO_REF_FILTER}. Set when opening from {@code partition.getQuerySequence()}.
     */
    private int bgzfRefFilterIndex = NO_REF_FILTER;

    // Indexed VFO-split mode — the reader seeks straight to a BAI record-start VFO and stops when
    // its file pointer reaches endVfo (exact, index-guided; no block-boundary guessing).
    private boolean indexedVfoMode = false;
    private long indexedEndVfo;

    // SAM line-split mode — active when partition.isSamSplit()
    private boolean isSamSplitMode = false;
    private SAMLineParser samLineParser;  // null → empty partition
    private long samEndByte;

    public BamPartitionReader(BamInputPartition partition, boolean includeAttributes) {
        this(partition, includeAttributes, false);
    }

    public BamPartitionReader(BamInputPartition partition, boolean includeAttributes,
                              boolean includeFileMetadata) {
        log.trace("BamPartitionReader(path={}, includeAttributes={}, includeFileMetadata={})",
                partition.getPath(), includeAttributes, includeFileMetadata);
        this.partition = partition;
        this.includeAttributes = includeAttributes;
        this.includeFileMetadata = includeFileMetadata;
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
        if (rowsRead >= partition.rowLimit()) return false;
        boolean hasNext = nextRecord();
        if (hasNext) rowsRead++;
        return hasNext;
    }

    private boolean nextRecord() throws IOException {
        // SAM line-split mode: read one text line per record, stop at endByte.
        if (isSamSplitMode) {
            if (samLineParser == null) return false; // empty partition
            while (true) {
                long lineStart = fsInputStream.getPos();
                if (samEndByte != Long.MAX_VALUE && lineStart >= samEndByte) return false;
                String line = readSamLine();
                if (line == null) return false; // EOF
                if (line.isEmpty() || line.startsWith("@")) continue; // header or blank
                current = samLineParser.parseLine(line);
                log.trace("nextRecord() SAM split -> readName={}", current.getReadName());
                return true;
            }
        }

        // BGZF/VFO split mode: use BAMRecordCodec with an end-boundary stop condition. The stop is
        // checked before each decode, so a record that begins before the boundary is read in full
        // (the codec transparently reads into the next block to finish a straddling record).
        if (bcis != null) {
            if (bamRecordCodec == null) return false; // empty partition (no clean record start found)
            long endByte = partition.getEndByte();
            while (true) {
                if (indexedVfoMode) {
                    // Exact VFO stop: read records whose start VFO is < endVfo.
                    if (bcis.getFilePointer() >= indexedEndVfo) return false;
                } else if (endByte != Long.MAX_VALUE) {
                    long blockAddr = BlockCompressedFilePointerUtil.getBlockAddress(bcis.getFilePointer());
                    if (blockAddr >= endByte) {
                        log.trace("nextRecord() BGZF split: reached end boundary blockAddr={} endByte={}", blockAddr, endByte);
                        return false;
                    }
                }
                SAMRecord r = (SAMRecord) bamRecordCodec.decode();
                if (r == null) return false;
                // Indexed BGZF split (hybrid): keep only records of the partition's reference.
                // Byte ranges of adjacent references overlap at a shared boundary block, so the
                // reference filter is what makes the per-reference union exact (no dup, no miss).
                if (bgzfRefFilterIndex != NO_REF_FILTER && r.getReferenceIndex() != bgzfRefFilterIndex) {
                    continue;
                }
                current = r;
                log.trace("nextRecord() BGZF split -> readName={}", current.getReadName());
                return true;
            }
        }

        if (iterator == null || !iterator.hasNext()) return false;
        current = iterator.next();
        log.trace("nextRecord() -> readName={}", current.getReadName());
        return true;
    }

    @Override
    public InternalRow get() {
        log.trace("get() readName={}", current.getReadName());
        Object[] values = new Object[includeFileMetadata ? 14 : 13];
        values[0]  = toUTF8(current.getReadName());
        values[1]  = current.getFlags();
        values[2]  = toUTF8(current.getContig());
        long startPos = current.getAlignmentStart();
        values[3]  = startPos;                                 // LongType
        values[4]  = current.getMappingQuality();
        values[5]  = toUTF8(current.getCigarString());
        values[6]  = toUTF8(mateContig(current));
        values[7]  = (long) current.getMateAlignmentStart();  // LongType
        values[8]  = current.getInferredInsertSize();
        values[9]  = toUTF8(current.getReadString());
        values[10] = toUTF8(current.getBaseQualityString());
        values[11] = includeAttributes ? buildAttributesMap(current) : null; // map values are SAM TYPE:VALUE strings
        values[12] = startPos > 0 ? startPos - 1L : null;     // 0-based, null for unmapped
        if (includeFileMetadata) values[13] = fileMetadataRow; // _metadata struct (see FileMetadata.TYPE)
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

        // Compute the _metadata struct once, before dispatching to a mode-specific opener
        // (several of which return early). get() does not throw, so this stat must happen here.
        if (includeFileMetadata) {
            // partition.getIndexPath() is non-null only on index-driven partitions (VFO / region / unmapped).
            fileMetadataRow = FileMetadata.row(
                    partition.getHadoopConf(), partition.getPath(), partition.getIndexPath());
        }

        // CRAM container-split mode: checked first since isCram partitions never use BGZF/SAM paths.
        if (partition.getCramContainerSpans() != null) {
            log.trace("open() CRAM container-split mode spans.length={}",
                    partition.getCramContainerSpans().length);
            openCramContainerSplit();
            return;
        }

        // Indexed VFO-split mode: startByte/endByte carry virtual file offsets (record-start
        // boundaries from the BAI). Checked before BGZF byte-split since both set startByte/endByte.
        if (partition.isIndexedVfoSplit()) {
            log.trace("open() indexed VFO split startVfo={} endVfo={}",
                    partition.getStartByte(), partition.getEndByte());
            openIndexedVfoSplit();
            return;
        }

        // SAM line-split mode: must be checked before BGZF detection because both use startByte/endByte.
        if (partition.isSamSplit()) {
            log.trace("open() SAM line-split mode startByte={} endByte={}",
                    partition.getStartByte(), partition.getEndByte());
            openSamSplit();
            return;
        }

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
     * Opens the partition in CRAM container-split mode.
     *
     * <p>Creates a {@link CRAMIterator} over the container byte-span pair stored in
     * {@link BamInputPartition#getCramContainerSpans()}.  The iterator handles all
     * reference-lookup and record decoding internally; the existing {@link #next()} /
     * {@link #close()} paths work without modification because {@code iterator} is a plain
     * {@link SAMRecordIterator}.  An empty spans array ({@code length == 0}) leaves
     * {@code iterator} null, producing an empty partition.
     */
    private void openCramContainerSplit() throws IOException {
        long[] spans = partition.getCramContainerSpans();
        if (spans.length == 0) {
            log.trace("openCramContainerSplit() empty spans — empty partition");
            return;
        }

        Configuration conf = partition.getHadoopConf();
        Path cramPath = new Path(partition.getPath());
        FileSystem fs = cramPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(cramPath).getLen();

        fsInputStream = fs.open(cramPath);
        boolean success = false;
        try {
            HadoopSeekableStream seekable = new HadoopSeekableStream(
                    fsInputStream, fileLength, partition.getPath());

            ReferenceSource referenceSource;
            String referenceFile = partition.getReferenceFile();
            if (referenceFile != null && !"none".equals(partition.getReferenceMode())) {
                log.trace("openCramContainerSplit() CRAM with referenceFile={}", referenceFile);
                referenceSource = new ReferenceSource(new java.io.File(referenceFile));
            } else {
                log.trace("openCramContainerSplit() CRAM with no external reference");
                referenceSource = new ReferenceSource((java.io.File) null);
            }

            iterator = new CRAMIterator(seekable, referenceSource, ValidationStringency.LENIENT,
                    null, spans);
            log.trace("openCramContainerSplit() CRAMIterator opened spans=[{}, {}]", spans[0], spans[1]);
            success = true;
        } finally {
            if (!success) {
                try { fsInputStream.close(); } catch (IOException e) {
                    log.debug("suppressed exception closing CRAM stream", e);
                }
            }
        }
    }

    /**
     * Parsed BAM header plus the virtual file offset of the first alignment record.
     */
    private static final class BamHeaderInfo {
        final SAMFileHeader header;
        final long firstDataVfo;
        BamHeaderInfo(SAMFileHeader header, long firstDataVfo) {
            this.header = header;
            this.firstDataVfo = firstDataVfo;
        }
    }

    /**
     * Parses the BAM header from byte 0 via a temporary BGZF stream, returning the
     * {@link SAMFileHeader} and the virtual file offset where the first alignment record begins.
     */
    private BamHeaderInfo parseBgzfHeader(FileSystem fs, Path bamPath, long fileLength) throws IOException {
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
            long firstDataVFO = hdrBcis.getFilePointer();

            SAMTextHeaderCodec headerCodec = new SAMTextHeaderCodec();
            headerCodec.setValidationStringency(ValidationStringency.LENIENT);
            SAMFileHeader header = headerCodec.decode(
                    new StringLineReader(new String(headerText, java.nio.charset.StandardCharsets.US_ASCII)),
                    partition.getPath());
            hdrBcis.close();
            return new BamHeaderInfo(header, firstDataVFO);
        } finally {
            hdrStream.close();
        }
    }

    /**
     * Opens the partition in indexed VFO-split mode: seeks straight to {@code startByte} (a BAI
     * record-start virtual file offset) and reads records until the file pointer reaches
     * {@code endByte} (also a VFO), keeping only records of {@code querySequence}. Because both
     * boundaries are exact record-start VFOs from the index, no block-boundary guessing is needed
     * and the per-reference union across splits is exact.
     */
    private void openIndexedVfoSplit() throws IOException {
        Configuration conf = partition.getHadoopConf();
        Path bamPath = new Path(partition.getPath());
        FileSystem fs = bamPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(bamPath).getLen();

        SAMFileHeader header = parseBgzfHeader(fs, bamPath, fileLength).header;
        bgzfRefFilterIndex = partition.getQuerySequence() != null
                ? header.getSequenceIndex(partition.getQuerySequence()) : NO_REF_FILTER;
        indexedVfoMode = true;
        indexedEndVfo = partition.getEndByte();

        bgzfFsInputStream = fs.open(bamPath);
        boolean success = false;
        try {
            bcis = new BlockCompressedInputStream(
                    new HadoopSeekableStream(bgzfFsInputStream, fileLength, partition.getPath()));
            bcis.seek(partition.getStartByte()); // startByte carries the start VFO (a record boundary)
            bamRecordCodec = new BAMRecordCodec(header);
            bamRecordCodec.setInputStream(bcis, partition.getPath());
            success = true;
        } finally {
            if (!success) {
                if (bcis != null) {
                    try { bcis.close(); } catch (IOException e) { log.debug("suppressed bcis close", e); }
                } else {
                    bgzfFsInputStream.close();
                }
            }
        }
    }

    /**
     * Opens the partition in BGZF split mode.
     *
     * <p>Reads the BAM header from byte 0 using a temporary stream, then opens a second stream
     * positioned at the first BAM record that starts at or after this partition's {@code startByte},
     * located by {@link #guessFirstRecordVfo} (which finds record starts <em>within</em> a BGZF
     * block, not only at block boundaries). Partitions where no record start exists in range are
     * marked empty ({@link #bamRecordCodec} remains null).
     *
     * <p>A record is owned by exactly one partition — the one whose byte range contains the start of
     * the BGZF block holding the record's first byte — so the split is lossless even when records
     * straddle every block boundary.
     */
    private void openBgzfSplit() throws IOException {
        Configuration conf = partition.getHadoopConf();
        Path bamPath = new Path(partition.getPath());
        FileSystem fs = bamPath.getFileSystem(conf);
        long fileLength = fs.getFileStatus(bamPath).getLen();

        // ── Step A: parse BAM header from byte 0 to get SAMFileHeader + firstDataVFO ──
        BamHeaderInfo hdr = parseBgzfHeader(fs, bamPath, fileLength);
        SAMFileHeader header = hdr.header;
        long firstDataVFO = hdr.firstDataVfo;

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
                startVFO = guessFirstRecordVfo(
                        bgzfFsInputStream, bcis, fileLength,
                        partition.getStartByte(),
                        partition.getEndByte(),
                        header.getSequenceDictionary().size());
                if (startVFO < 0) {
                    log.trace("openBgzfSplit() no record start found in range — empty partition");
                    bamRecordCodec = null;
                    success = true;
                    return;
                }
                log.trace("openBgzfSplit() first record vfo={}", startVFO);
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
     * Opens the partition in SAM line-split mode.
     *
     * <p>Reads the SAM header from byte 0 using a temporary stream (to obtain the
     * {@link SAMFileHeader} needed by {@link SAMLineParser}), then opens a second stream
     * positioned at the first data line within this partition's byte range.
     *
     * <ul>
     *   <li>Partition 0 ({@code startByte == 0}): seeks to {@code dataStartByte}, the byte
     *       immediately after the last {@code @}-prefixed header line.</li>
     *   <li>Partitions &gt; 0: seeks to {@code startByte}, discards bytes up to and including
     *       the next {@code \n} (we may have landed mid-line), then begins reading.</li>
     * </ul>
     *
     * <p>If the resulting position is at or past {@code endByte}, the partition is marked empty
     * ({@link #samLineParser} remains null) and {@link #next()} returns false immediately.
     */
    private void openSamSplit() throws IOException {
        isSamSplitMode = true;
        samEndByte = partition.getEndByte();

        Configuration conf = partition.getHadoopConf();
        Path samPath = new Path(partition.getPath());
        FileSystem fs = samPath.getFileSystem(conf);

        // ── Step A: parse SAM header from byte 0 ──
        // Use BufferedInputStream to amortize per-byte read overhead on cloud streams.
        // Position is tracked manually because BufferedInputStream's read-ahead makes
        // FSDataInputStream.getPos() unreliable for logical line boundaries.
        SAMFileHeader header;
        long dataStartByte;
        FSDataInputStream hdrStream = fs.open(samPath);
        try {
            BufferedInputStream bufferedHdr = new BufferedInputStream(hdrStream, 65536);
            StringBuilder headerText = new StringBuilder();
            long pos = 0;
            long lineEndPos = 0;
            while (true) {
                long lineStartPos = pos;
                StringBuilder lineBuilder = new StringBuilder(256);
                boolean anyBytes = false;
                int b;
                while ((b = bufferedHdr.read()) != -1) {
                    pos++;
                    anyBytes = true;
                    if (b == '\n') break;
                    if (b != '\r') lineBuilder.append((char) b);
                }
                if (!anyBytes) {
                    // File is entirely header (or empty) — data starts at EOF.
                    dataStartByte = lineEndPos;
                    break;
                }
                String line = lineBuilder.toString();
                if (line.startsWith("@")) {
                    headerText.append(line).append('\n');
                    lineEndPos = pos;
                } else {
                    // First non-header line: data starts at lineStartPos.
                    dataStartByte = lineStartPos;
                    break;
                }
            }
            SAMTextHeaderCodec headerCodec = new SAMTextHeaderCodec();
            headerCodec.setValidationStringency(ValidationStringency.LENIENT);
            header = headerCodec.decode(
                    new StringLineReader(headerText.toString()), partition.getPath());
            log.trace("openSamSplit() header parsed, dataStartByte={}", dataStartByte);
        } finally {
            hdrStream.close();
        }

        // ── Step B: open data stream and seek to our start position ──
        long fileLength = fs.getFileStatus(samPath).getLen();

        // Guard: startByte past EOF → empty partition (avoid seek-after-EOF exception).
        if (partition.getStartByte() > 0 && partition.getStartByte() >= fileLength) {
            log.trace("openSamSplit() startByte={} >= fileLength={} — empty partition",
                    partition.getStartByte(), fileLength);
            return; // samLineParser stays null
        }

        fsInputStream = fs.open(samPath);
        boolean success = false;
        try {
            if (partition.getStartByte() == 0) {
                // Partition 0: skip over the header to the first data line.
                fsInputStream.seek(dataStartByte);
            } else {
                // Partitions > 0: peek at the byte immediately before startByte.
                // If it is '\n', startByte falls exactly on a line boundary and no
                // skipping is needed.  Otherwise we landed mid-line and must discard
                // through the next '\n'.
                fsInputStream.seek(partition.getStartByte() - 1);
                int prev = fsInputStream.read(); // advances position to startByte
                if (prev != '\n') {
                    int b;
                    while ((b = fsInputStream.read()) != -1 && b != '\n') { /* discard partial line */ }
                }
            }

            long pos = fsInputStream.getPos();
            if (samEndByte != Long.MAX_VALUE && pos >= samEndByte) {
                log.trace("openSamSplit() position {} already at or past endByte {} — empty partition",
                        pos, samEndByte);
                // samLineParser stays null → empty partition
                success = true;
                return;
            }

            samLineParser = new SAMLineParser(header);
            log.trace("openSamSplit() ready at position={}", pos);
            success = true;
        } finally {
            if (!success && fsInputStream != null) fsInputStream.close();
        }
    }

    /**
     * Reads one line byte-by-byte from the given stream, returning the line content
     * without the trailing {@code \n} (or {@code \r\n}). Returns null at EOF when
     * no bytes were read.
     *
     * <p>Byte-by-byte reading ensures {@link FSDataInputStream#getPos()} accurately
     * reflects the start of each line before the call — no buffering layer obscures
     * the position.
     */
    private static String readLineFrom(FSDataInputStream stream) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        int b;
        while ((b = stream.read()) != -1) {
            if (b == '\n') return sb.toString();
            if (b != '\r') sb.append((char) b);
        }
        return sb.length() > 0 ? sb.toString() : null;
    }

    /**
     * Reads one SAM data line from {@link #fsInputStream}.
     * Delegates to {@link #readLineFrom(FSDataInputStream)} using the partition's data stream.
     */
    private String readSamLine() throws IOException {
        return readLineFrom(fsInputStream);
    }

    /** Number of consecutive self-consistent records required to accept a candidate record start. */
    private static final int GUESS_VALIDATE_RECORDS = 3;
    /** Fixed BAM record header bytes parsed for validation: block_size(4) + the 32-byte fixed block. */
    private static final int BAM_RECORD_FIXED = 36;

    /**
     * Finds the virtual file offset of the first BAM record that begins at or after
     * {@code searchFrom} (in the compressed file), for an unindexed BGZF split. Unlike a
     * block-boundary search, the record start may lie <em>within</em> a BGZF block — which is the
     * normal case, because the writer flushes 64 KB blocks mid-record, so most blocks begin with the
     * tail of a straddling record. Without locating these in-block starts, every partition after the
     * first would fail to orient and silently drop its records.
     *
     * <p>For each contiguous BGZF block at or after {@code searchFrom}, every uncompressed offset in
     * the block is tried as a candidate record start and accepted only if it and the following
     * {@value #GUESS_VALIDATE_RECORDS} records all parse as self-consistent BAM records (in-range
     * {@code refID}/{@code pos}/{@code next_refID}/{@code next_pos} and a {@code block_size}
     * consistent with the name/cigar/seq/qual lengths), or fewer at a clean end of file. Requiring a
     * run of valid records makes a coincidental mid-record match effectively impossible.
     *
     * @return the record-start virtual file offset, or -1 if none is found before {@code endByte}
     */
    private long guessFirstRecordVfo(FSDataInputStream rawStream,
                                     BlockCompressedInputStream bcis,
                                     long fileLength,
                                     long searchFrom,
                                     long endByte,
                                     int numRefs) throws IOException {
        long cp = findNextBgzfBlockStart(rawStream, searchFrom);
        while (cp >= 0 && cp < fileLength && (endByte == Long.MAX_VALUE || cp < endByte)) {
            long[] block = parseBgzfBlock(rawStream, cp, fileLength); // {compressedSize, uncompressedSize}
            if (block == null) {
                // Not a real block boundary (e.g. magic bytes inside compressed data); keep scanning.
                cp = findNextBgzfBlockStart(rawStream, cp + 1);
                continue;
            }
            int uncompressed = (int) block[1];
            for (int u = 0; u < uncompressed; u++) {
                long vfo = BlockCompressedFilePointerUtil.makeFilePointer(cp, u);
                if (isRecordStart(bcis, vfo, numRefs)) {
                    return vfo;
                }
            }
            cp += block[0]; // contiguous next block
        }
        return -1L;
    }

    /**
     * Parses the BGZF block header (and gzip footer) at raw offset {@code cp} without decompressing,
     * returning {@code {compressedBlockSize, uncompressedSize}}, or null if {@code cp} is not a valid
     * BGZF block start.
     */
    private static long[] parseBgzfBlock(FSDataInputStream stream, long cp, long fileLength) throws IOException {
        if (cp + 18 > fileLength) return null;
        stream.seek(cp);
        byte[] head = new byte[12];
        if (readFully(stream, head) < 12) return null;
        if (head[0] != BGZF_MAGIC[0] || head[1] != BGZF_MAGIC[1]
                || head[2] != BGZF_MAGIC[2] || head[3] != BGZF_MAGIC[3]) return null;
        int xlen = (head[10] & 0xff) | ((head[11] & 0xff) << 8);
        byte[] extra = new byte[xlen];
        if (readFully(stream, extra) < xlen) return null;
        int bsize = -1;
        for (int i = 0; i + 4 <= xlen; ) {
            int si1 = extra[i] & 0xff;
            int si2 = extra[i + 1] & 0xff;
            int slen = (extra[i + 2] & 0xff) | ((extra[i + 3] & 0xff) << 8);
            if (si1 == 66 && si2 == 67 && slen == 2 && i + 6 <= xlen) {
                bsize = (extra[i + 4] & 0xff) | ((extra[i + 5] & 0xff) << 8);
                break;
            }
            i += 4 + slen;
        }
        if (bsize < 0) return null;
        long compressedSize = bsize + 1L;          // BGZF: total block size is BSIZE + 1
        if (cp + compressedSize > fileLength) return null;
        stream.seek(cp + compressedSize - 4);       // ISIZE = last 4 bytes (uncompressed length)
        byte[] isize = new byte[4];
        if (readFully(stream, isize) < 4) return null;
        long uncompressedSize = le32(isize, 0) & 0xffffffffL;
        return new long[]{compressedSize, uncompressedSize};
    }

    /**
     * Returns true if a run of {@value #GUESS_VALIDATE_RECORDS} self-consistent BAM records begins at
     * {@code vfo} (or fewer, ending at a clean end of file). Used by {@link #guessFirstRecordVfo}.
     */
    private static boolean isRecordStart(BlockCompressedInputStream bcis, long vfo, int numRefs)
            throws IOException {
        bcis.seek(vfo);
        byte[] h = new byte[BAM_RECORD_FIXED];
        for (int i = 0; i < GUESS_VALIDATE_RECORDS; i++) {
            int n = readFully(bcis, h);
            if (n == 0) return i >= 1;            // clean EOF at a record boundary after >=1 record
            if (n < BAM_RECORD_FIXED) return false; // truncated header → not a valid start here
            int blockSize = le32(h, 0);
            int refId     = le32(h, 4);
            int pos       = le32(h, 8);
            int lReadName = h[12] & 0xff;
            int nCigar    = (h[16] & 0xff) | ((h[17] & 0xff) << 8);
            int lSeq      = le32(h, 20);
            int nextRefId = le32(h, 24);
            int nextPos   = le32(h, 28);
            if (!(blockSize >= MIN_BAM_RECORD_BODY && blockSize <= MAX_BAM_RECORD_BODY
                    && refId >= -1 && refId < numRefs
                    && nextRefId >= -1 && nextRefId < numRefs
                    && pos >= -1 && nextPos >= -1
                    && lReadName >= 1 && lSeq >= 0 && nCigar >= 0)) {
                return false;
            }
            // block_size must be at least the fixed 32 bytes after it + name + cigar + seq + qual.
            long minBytes = 32L + lReadName + 4L * nCigar + (lSeq + 1) / 2 + lSeq;
            if (blockSize < minBytes) return false;
            // Advance to the next record: block_size counts bytes after the length field; 32 read here.
            if (!skipFully(bcis, blockSize - 32L)) return false; // claimed length runs past EOF → invalid
        }
        return true;
    }

    /** Skips exactly {@code n} bytes from {@code bcis} by reading and discarding; false if EOF first. */
    private static boolean skipFully(BlockCompressedInputStream bcis, long n) throws IOException {
        byte[] discard = new byte[(int) Math.min(n, 8192L)];
        long remaining = n;
        while (remaining > 0) {
            int r = bcis.read(discard, 0, (int) Math.min(discard.length, remaining));
            if (r < 0) return false;
            remaining -= r;
        }
        return true;
    }

    /** Reads up to {@code buf.length} bytes, returning the count actually read (handles short reads). */
    private static int readFully(BlockCompressedInputStream bcis, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int r = bcis.read(buf, total, buf.length - total);
            if (r < 0) break;
            total += r;
        }
        return total;
    }

    /** Reads up to {@code buf.length} bytes from a raw stream, returning the count actually read. */
    private static int readFully(FSDataInputStream stream, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int r = stream.read(buf, total, buf.length - total);
            if (r < 0) break;
            total += r;
        }
        return total;
    }

    /** Little-endian int32 decode at {@code off}. */
    private static int le32(byte[] b, int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8)
                | ((b[off + 2] & 0xff) << 16) | ((b[off + 3] & 0xff) << 24);
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

    /**
     * Builds the {@code attributes} map, encoding each tag value as a SAM {@code TYPE:VALUE}
     * string (e.g. {@code i:0}, {@code Z:group1}, {@code A:P}, {@code f:3.14},
     * {@code B:i,1,2,3}) so downstream consumers can recover the SAM type and parse safely
     * rather than receiving a bare, type-erased value.
     *
     * <p>The encoding is produced by htsjdk's own {@link TextTagCodec}, which yields
     * {@code TAG:TYPE:VALUE} (identical to a {@code samtools view} optional field); we strip the
     * leading {@code TAG:} prefix. This inherits htsjdk's public-API behaviour: integer subtypes
     * collapse to {@code i}, hex ({@code H}) is rendered as a {@code B} byte array, and the
     * signed/unsigned array distinction is not preserved (lost by {@link SAMRecord#getAttributes()}).
     */
    private ArrayBasedMapData buildAttributesMap(SAMRecord record) {
        List<SAMRecord.SAMTagAndValue> attrs = record.getAttributes();
        int n = attrs.size();
        UTF8String[] keys   = new UTF8String[n];
        UTF8String[] values = new UTF8String[n];
        for (int i = 0; i < n; i++) {
            SAMRecord.SAMTagAndValue tv = attrs.get(i);
            keys[i]   = UTF8String.fromString(tv.tag);
            // TextTagCodec.encode returns "TAG:TYPE:VALUE"; drop the 2-char tag plus its colon.
            String encoded = tagCodec.encode(tv.tag, tv.value);
            values[i] = UTF8String.fromString(encoded.substring(tv.tag.length() + 1));
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
    }
}
