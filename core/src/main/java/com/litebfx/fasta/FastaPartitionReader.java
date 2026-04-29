package com.litebfx.fasta;

import com.litebfx.HadoopSeekableStream;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

/**
 * Reads one or all contigs from a FASTA file and converts them to Spark
 * {@link InternalRow}s matching {@link FastaSchema#SCHEMA}.
 *
 * <h3>Indexed (per-contig) mode — local paths</h3>
 * Uses htsjdk {@code ReferenceSequenceFileFactory} (NIO path) which auto-discovers
 * the co-located {@code .fai} and seeks to the contig offset.
 *
 * <h3>Indexed (per-contig) mode — remote paths (S3A, HDFS, …)</h3>
 * Uses {@link HadoopSeekableStream} over the FASTA file and reads the FAI via
 * Hadoop FS to determine the byte offset.  This issues a single HTTP Range
 * request for the contig rather than streaming the full file.
 *
 * <h3>Full-scan mode</h3>
 * When {@code contigName} is null, iterates all sequences sequentially (htsjdk
 * for local, line-by-line Hadoop reader for remote).
 */
public class FastaPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger log = LoggerFactory.getLogger(FastaPartitionReader.class);

    private final FastaInputPartition partition;

    // -- local (NIO / htsjdk) state --
    private ReferenceSequenceFile refFile;

    // -- remote (Hadoop) state --
    private HadoopSeekableStream hadoopStream;   // indexed mode
    private FSDataInputStream    hadoopFsIn;     // backing stream for hadoopStream
    private FaiEntry             hadoopFaiEntry; // FAI entry for the target contig
    private BufferedReader       hadoopLineReader; // full-scan mode
    private String               hadoopBufferedHeader; // look-ahead for full-scan

    private ReferenceSequence current;
    private boolean done = false;

    public FastaPartitionReader(FastaInputPartition partition) {
        log.trace("FastaPartitionReader(path={}, contigName={})",
                partition.getPath(), partition.getContigName());
        this.partition = partition;
    }

    /** Package-private for testing the Hadoop full-scan path without a real FS. */
    FastaPartitionReader(FastaInputPartition partition, java.io.BufferedReader lineReader) {
        this.partition = partition;
        this.hadoopLineReader = lineReader;
    }

    /** Package-private for testing the Hadoop indexed-read path without a real remote FS. */
    FastaPartitionReader(FastaInputPartition partition,
                         HadoopSeekableStream hadoopStream,
                         FaiEntry faiEntry) {
        this.partition      = partition;
        this.hadoopStream   = hadoopStream;
        this.hadoopFaiEntry = faiEntry;
    }

    @Override
    public boolean next() throws IOException {
        if (!isOpened()) open();

        if (partition.getContigName() != null) {
            // Indexed mode — one record per partition.
            if (done) return false;
            if (hadoopStream != null) {
                current = readContigViaHadoop();
            } else if (refFile != null) {
                current = refFile.getSequence(partition.getContigName());
            } else {
                throw new IllegalStateException("No reader opened for indexed FASTA mode");
            }
            done = true;
            log.trace("next() indexed contig={}", partition.getContigName());
            return current != null;
        } else {
            // Full-scan mode.
            if (hadoopLineReader != null) {
                current = readNextContigViaHadoop();
            } else if (refFile != null) {
                current = refFile.nextSequence();
            } else {
                throw new IllegalStateException("No reader opened for full-scan FASTA mode");
            }
            if (current != null) log.trace("next() full-scan contig={}", current.getName());
            return current != null;
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
        // These three cases are mutually exclusive; use else-if to avoid double-closing
        // hadoopFsIn (which is owned by hadoopStream and closed when hadoopStream is closed).
        if (refFile != null)               refFile.close();
        else if (hadoopLineReader != null) hadoopLineReader.close();
        else if (hadoopStream != null)     hadoopStream.close();
    }

    // -------------------------------------------------------------------------
    // Open
    // -------------------------------------------------------------------------

    private boolean isOpened() {
        return refFile != null || hadoopStream != null || hadoopLineReader != null;
    }

    private void open() throws IOException {
        String pathStr = partition.getPath();
        log.trace("open() path={}", pathStr);
        if (isLocalPath(pathStr)) {
            openWithNio(pathStr);
        } else {
            openWithHadoop(pathStr);
        }
    }

    private void openWithNio(String pathStr) throws IOException {
        java.nio.file.Path nioPath = toNioPath(pathStr);
        log.trace("openWithNio() nioPath={}", nioPath);
        refFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(nioPath);
        log.trace("openWithNio() isIndexed={}", refFile.isIndexed());
    }

    /**
     * Opens the FASTA via Hadoop FileSystem.
     *
     * <ul>
     *   <li>Indexed mode ({@code contigName != null}): opens a
     *       {@link HadoopSeekableStream} for random-seek access and reads the
     *       FAI entry for the target contig.</li>
     *   <li>Full-scan mode ({@code contigName == null}): opens a
     *       {@link BufferedReader} for sequential line-by-line iteration.</li>
     * </ul>
     */
    private void openWithHadoop(String pathStr) throws IOException {
        org.apache.hadoop.conf.Configuration conf = partition.getHadoopConf();
        Path hadoopPath = new Path(pathStr);
        FileSystem fs = hadoopPath.getFileSystem(conf);

        if (partition.getContigName() != null && partition.getFaiPath() != null) {
            // Indexed mode — seekable stream + FAI lookup
            long fileLen = fs.getFileStatus(hadoopPath).getLen();
            hadoopFsIn = fs.open(hadoopPath);
            hadoopStream = new HadoopSeekableStream(hadoopFsIn, fileLen, pathStr);
            hadoopFaiEntry = readFaiEntry(partition.getFaiPath(), partition.getContigName(), conf);
            log.trace("openWithHadoop() indexed contig={} offset={}", partition.getContigName(), hadoopFaiEntry.offset);
        } else {
            // Full-scan mode — sequential stream
            FSDataInputStream in = fs.open(hadoopPath);
            hadoopLineReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            log.trace("openWithHadoop() full-scan");
        }
    }

    // -------------------------------------------------------------------------
    // Remote (Hadoop) reading
    // -------------------------------------------------------------------------

    /**
     * Seeks to the contig's byte offset in the FASTA and reads all contig bases.
     *
     * <h3>Coalesced read (newlineSize &le; maxMergeGap)</h3>
     * Issues one {@code seek()} and one contiguous {@code read()} covering all
     * raw bytes (bases + inter-line newlines), then strips newlines in memory.
     * This produces a single HTTP Range request on S3A, which is optimal when the
     * gaps between lines are small (standard LF/CRLF newlines).
     *
     * <h3>Per-line reads (newlineSize &gt; maxMergeGap)</h3>
     * Issues one {@code seek()} + {@code read()} per base line, skipping over
     * inter-line gaps entirely.  Use this when the gap between lines is large
     * enough that fetching it would waste significant bandwidth.
     *
     * <p>The threshold is controlled by the {@code maxMergeGap} DataSource option
     * (default {@link FastaInputPartition#DEFAULT_MAX_MERGE_GAP}).
     */
    private ReferenceSequence readContigViaHadoop() throws IOException {
        FaiEntry fai = hadoopFaiEntry;
        int numLines    = (int) Math.ceil((double) fai.size / fai.basesPerLine);
        int newlineSize = fai.bytesPerLine - fai.basesPerLine;
        byte[] bases    = new byte[(int) fai.size];

        if (newlineSize <= partition.getMaxMergeGap()) {
            // Coalesced path: one seek + one contiguous read, strip newlines in memory.
            int totalRawBytes = (int) fai.size + (numLines - 1) * newlineSize;
            hadoopStream.seek(fai.offset);
            byte[] raw = new byte[totalRawBytes];
            int remaining = totalRawBytes;
            while (remaining > 0) {
                int n = hadoopStream.read(raw, totalRawBytes - remaining, remaining);
                if (n < 0) throw new IOException("Unexpected EOF reading FASTA contig data");
                remaining -= n;
            }
            int basePos = 0, rawPos = 0;
            while (basePos < bases.length) {
                int lineBases = Math.min(fai.basesPerLine, bases.length - basePos);
                System.arraycopy(raw, rawPos, bases, basePos, lineBases);
                basePos += lineBases;
                rawPos  += lineBases;
                if (basePos < bases.length) rawPos += newlineSize;
            }
        } else {
            // Per-line path: seek to each line, skip inter-line gaps.
            int basePos = 0;
            long lineOffset = fai.offset;
            while (basePos < bases.length) {
                int lineBases = Math.min(fai.basesPerLine, bases.length - basePos);
                hadoopStream.seek(lineOffset);
                int remaining = lineBases;
                while (remaining > 0) {
                    int n = hadoopStream.read(bases, basePos + (lineBases - remaining), remaining);
                    if (n < 0) throw new IOException("Unexpected EOF reading FASTA contig data");
                    remaining -= n;
                }
                basePos    += lineBases;
                lineOffset += fai.bytesPerLine;
            }
        }
        return new ReferenceSequence(partition.getContigName(), 0, bases);
    }

    /**
     * Reads the next FASTA record from a sequential {@link BufferedReader}.
     * Returns null at end of file.
     */
    ReferenceSequence readNextContigViaHadoop() throws IOException {
        // Consume or re-use the buffered header from the previous call
        String header = hadoopBufferedHeader;
        hadoopBufferedHeader = null;

        if (header == null) {
            header = hadoopLineReader.readLine();
            while (header != null && !header.startsWith(">")) {
                header = hadoopLineReader.readLine();
            }
        }
        if (header == null) return null; // EOF

        String name = header.substring(1).split("\\s+", 2)[0];
        StringBuilder seqBuilder = new StringBuilder();
        String line;
        while ((line = hadoopLineReader.readLine()) != null) {
            if (line.startsWith(">")) {
                hadoopBufferedHeader = line;
                break;
            }
            seqBuilder.append(line.trim());
        }

        byte[] bases = seqBuilder.toString().getBytes(StandardCharsets.US_ASCII);
        return new ReferenceSequence(name, 0, bases);
    }

    // -------------------------------------------------------------------------
    // FAI parsing
    // -------------------------------------------------------------------------

    /**
     * Reads the FAI file via Hadoop FS and returns the index entry for
     * {@code contigName}.
     * <p>FAI format: NAME\tLENGTH\tOFFSET\tBASES_PER_LINE\tBYTES_PER_LINE
     */
    static FaiEntry readFaiEntry(String faiPath, String contigName,
                                 org.apache.hadoop.conf.Configuration conf)
            throws IOException {
        Path path = new Path(faiPath);
        try (FSDataInputStream in = path.getFileSystem(conf).open(path);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] parts = line.split("\t");
                if (parts[0].equals(contigName)) {
                    if (parts.length < 5) {
                        throw new IOException(String.format(
                            "Malformed FAI line for contig '%s' in %s " +
                            "(expected 5 tab-separated fields, got %d): %s",
                            contigName, faiPath, parts.length, line));
                    }
                    try {
                        return new FaiEntry(
                            Long.parseLong(parts[1]),    // size (bases)
                            Long.parseLong(parts[2]),    // offset (byte position of first base)
                            Integer.parseInt(parts[3]),  // basesPerLine
                            Integer.parseInt(parts[4])   // bytesPerLine (includes newline)
                        );
                    } catch (NumberFormatException e) {
                        throw new IOException(String.format(
                            "Malformed FAI entry for contig '%s' in %s: %s",
                            contigName, faiPath, line), e);
                    }
                }
            }
        }
        throw new IOException("Contig '" + contigName + "' not found in FAI: " + faiPath);
    }

    // -------------------------------------------------------------------------
    // Path helpers
    // -------------------------------------------------------------------------

    static boolean isLocalPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            String scheme = uri.getScheme();
            return scheme == null || scheme.equals("file");
        } catch (Exception e) {
            return true; // treat unparseable paths as local
        }
    }

    static java.nio.file.Path toNioPath(String pathStr) {
        try {
            URI uri = URI.create(pathStr);
            if (uri.getScheme() == null) {
                return Paths.get(pathStr);
            }
            return Paths.get(uri);
        } catch (Exception e) {
            return Paths.get(pathStr);
        }
    }

    // -------------------------------------------------------------------------
    // FAI entry value type
    // -------------------------------------------------------------------------

    static final class FaiEntry {
        final long size;         // total bases in contig
        final long offset;       // byte offset of first base in FASTA file
        final int  basesPerLine; // bases per wrapped line
        final int  bytesPerLine; // bytes per line (includes newline char(s))

        FaiEntry(long size, long offset, int basesPerLine, int bytesPerLine) {
            this.size         = size;
            this.offset       = offset;
            this.basesPerLine = basesPerLine;
            this.bytesPerLine = bytesPerLine;
        }
    }
}
