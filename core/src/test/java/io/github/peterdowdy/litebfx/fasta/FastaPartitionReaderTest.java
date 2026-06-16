package io.github.peterdowdy.litebfx.fasta;

import io.github.peterdowdy.litebfx.HadoopSeekableStream;
import htsjdk.samtools.reference.ReferenceSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FastaPartitionReader}.
 *
 * <p>Tests drive the reader directly (no SparkSession) using the {@code realn01.fa}
 * fixture from test resources plus in-memory fixtures for the Hadoop helper methods.
 *
 * <p>FAI entry for realn01.fa:
 * <pre>
 *   000000F  686  21  60  61
 * </pre>
 * (686 bases, offset 21, 60 bases/line, 61 bytes/line including LF)
 */
class FastaPartitionReaderTest {

    static final String CONTIG_NAME = "000000F";
    static final long   CONTIG_LEN  = 686L;
    static final long   FAI_OFFSET  = 21L;
    static final int    BASES_PER_LINE = 60;
    static final int    BYTES_PER_LINE = 61;

    // First 60 bases from line 2 of realn01.fa
    static final String EXPECTED_PREFIX =
            "CAGACAAACATACACCATCAGACAGCAGCACCATATTCTTTTTTTCTGCTAATTTGCTAA";

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // isLocalPath
    // -------------------------------------------------------------------------

    @Test
    void isLocalPath_nullScheme_isLocal() {
        assertTrue(FastaPartitionReader.isLocalPath("/some/absolute/path.fa"));
    }

    @Test
    void isLocalPath_fileScheme_isLocal() {
        assertTrue(FastaPartitionReader.isLocalPath("file:///some/path.fa"));
    }

    @Test
    void isLocalPath_s3aScheme_isRemote() {
        assertFalse(FastaPartitionReader.isLocalPath("s3a://bucket/file.fa"));
    }

    @Test
    void isLocalPath_hdfsScheme_isRemote() {
        assertFalse(FastaPartitionReader.isLocalPath("hdfs://namenode/file.fa"));
    }

    // -------------------------------------------------------------------------
    // toNioPath
    // -------------------------------------------------------------------------

    @Test
    void toNioPath_bareString_returnsPath() {
        java.nio.file.Path p = FastaPartitionReader.toNioPath("/tmp/foo.fa");
        assertEquals(java.nio.file.Paths.get("/tmp/foo.fa"), p);
    }

    @Test
    void toNioPath_fileUri_returnsPath() {
        java.nio.file.Path p = FastaPartitionReader.toNioPath("file:///tmp/foo.fa");
        assertEquals(java.nio.file.Paths.get("/tmp/foo.fa"), p);
    }

    // -------------------------------------------------------------------------
    // readFaiEntry
    // -------------------------------------------------------------------------

    @Test
    void readFaiEntry_validLine_parsesCorrectly() throws Exception {
        Path fai = tempDir.resolve("test.fa.fai");
        Files.writeString(fai, "000000F\t686\t21\t60\t61\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry entry = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "000000F", new Configuration());

        assertEquals(686L, entry.size);
        assertEquals(21L,  entry.offset);
        assertEquals(60,   entry.basesPerLine);
        assertEquals(61,   entry.bytesPerLine);
    }

    @Test
    void readFaiEntry_multiContigFai_returnsCorrectContig() throws Exception {
        Path fai = tempDir.resolve("multi.fa.fai");
        Files.writeString(fai,
                "chr1\t100\t6\t60\t61\n" +
                "chr2\t200\t171\t60\t61\n",
                StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry entry = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "chr2", new Configuration());

        assertEquals(200L, entry.size);
        assertEquals(171L, entry.offset);
    }

    @Test
    void readFaiEntry_missingContig_throwsIOException() throws Exception {
        Path fai = tempDir.resolve("missing.fa.fai");
        Files.writeString(fai, "chr1\t100\t6\t60\t61\n", StandardCharsets.UTF_8);

        IOException ex = assertThrows(IOException.class, () ->
                FastaPartitionReader.readFaiEntry(
                        fai.toUri().toString(), "chrX", new Configuration()));
        assertTrue(ex.getMessage().contains("chrX"));
    }

    @Test
    void readFaiEntry_malformedLine_throwsIOException() throws Exception {
        Path fai = tempDir.resolve("malformed.fa.fai");
        // Only 3 fields instead of 5
        Files.writeString(fai, "chr1\t100\t6\n", StandardCharsets.UTF_8);

        IOException ex = assertThrows(IOException.class, () ->
                FastaPartitionReader.readFaiEntry(
                        fai.toUri().toString(), "chr1", new Configuration()));
        assertTrue(ex.getMessage().toLowerCase().contains("malformed"));
    }

    @Test
    void readFaiEntry_emptyLines_skipped() throws Exception {
        Path fai = tempDir.resolve("empty.fa.fai");
        Files.writeString(fai, "\nchr1\t100\t6\t60\t61\n\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry entry = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "chr1", new Configuration());
        assertEquals(100L, entry.size);
    }

    // -------------------------------------------------------------------------
    // readNextContigViaHadoop — injected via package-private constructor
    // -------------------------------------------------------------------------

    private static FastaPartitionReader readerWithLineReader(BufferedReader br) {
        FastaInputPartition partition = new FastaInputPartition(
                "dummy://host/file.fa", null, null, new Configuration());
        return new FastaPartitionReader(partition, br);
    }

    @Test
    void readNextContigViaHadoop_emptyInput_returnsNull() throws Exception {
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader("")));
        ReferenceSequence seq = reader.readNextContigViaHadoop();
        assertNull(seq);
    }

    @Test
    void readNextContigViaHadoop_singleContig_returnsBases() throws Exception {
        String fasta = ">chr1 description\nACGT\nACGT\n";
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader(fasta)));

        ReferenceSequence seq = reader.readNextContigViaHadoop();
        assertNotNull(seq);
        assertEquals("chr1", seq.getName());
        assertEquals("ACGTACGT", new String(seq.getBases(), StandardCharsets.US_ASCII));
    }

    @Test
    void readNextContigViaHadoop_multiContig_iteratesAll() throws Exception {
        String fasta = ">chr1\nAAAA\n>chr2\nCCCC\n>chr3\nGGGG\n";
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader(fasta)));

        ReferenceSequence s1 = reader.readNextContigViaHadoop();
        ReferenceSequence s2 = reader.readNextContigViaHadoop();
        ReferenceSequence s3 = reader.readNextContigViaHadoop();
        ReferenceSequence s4 = reader.readNextContigViaHadoop();

        assertNotNull(s1);
        assertNotNull(s2);
        assertNotNull(s3);
        assertNull(s4);  // EOF

        assertEquals("chr1", s1.getName());
        assertEquals("chr2", s2.getName());
        assertEquals("chr3", s3.getName());

        assertEquals("AAAA", new String(s1.getBases(), StandardCharsets.US_ASCII));
        assertEquals("CCCC", new String(s2.getBases(), StandardCharsets.US_ASCII));
        assertEquals("GGGG", new String(s3.getBases(), StandardCharsets.US_ASCII));
    }

    @Test
    void readNextContigViaHadoop_noHeaderLine_returnsNull() throws Exception {
        // Content with only sequence lines and no '>' header — EOF without finding a header
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader("ACGT\nACGT\n")));
        ReferenceSequence seq = reader.readNextContigViaHadoop();
        assertNull(seq);
    }

    @Test
    void readNextContigViaHadoop_headerWithDescription_usesFirstToken() throws Exception {
        String fasta = ">contigA some description here\nTTTT\n";
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader(fasta)));

        ReferenceSequence seq = reader.readNextContigViaHadoop();
        assertNotNull(seq);
        assertEquals("contigA", seq.getName());
    }

    // -------------------------------------------------------------------------
    // Full reader via FastaInputPartition — NIO local path
    // -------------------------------------------------------------------------

    private static String realn01Path() throws Exception {
        URL url = FastaPartitionReaderTest.class.getClassLoader().getResource("realn01.fa");
        assertNotNull(url, "realn01.fa not found in test resources");
        return java.nio.file.Paths.get(url.toURI()).toAbsolutePath().toString();
    }

    @Test
    void localNio_indexedMode_nextReturnsTrue_getReturnsContig() throws Exception {
        String path = realn01Path();
        FastaInputPartition partition = new FastaInputPartition(
                path, CONTIG_NAME, null, new Configuration());

        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            assertTrue(reader.next());
            var row = reader.get();
            // Column 0: name, 1: sequence, 2: length
            assertEquals(CONTIG_NAME, row.getUTF8String(0).toString());
            assertEquals(CONTIG_LEN,  row.getLong(2));
            assertTrue(row.getUTF8String(1).toString().startsWith(EXPECTED_PREFIX));

            // Only one contig in indexed mode
            assertFalse(reader.next());
        }
    }

    @Test
    void localNio_fullScanMode_iteratesAllContigs() throws Exception {
        String path = realn01Path();
        // contigName=null → full-scan
        FastaInputPartition partition = new FastaInputPartition(
                path, null, null, new Configuration());

        int count = 0;
        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            while (reader.next()) {
                count++;
                var row = reader.get();
                // Every row must have a non-null name and a non-empty sequence
                assertFalse(row.getUTF8String(0).toString().isEmpty());
                assertTrue(row.getLong(2) > 0);
            }
        }
        // realn01.fa has exactly 1 contig
        assertEquals(1, count);
    }

    @Test
    void localNio_indexedMode_withMaxMergeGap_readsContig() throws Exception {
        String path = realn01Path();
        // Exercise the 5-arg constructor that sets maxMergeGap
        FastaInputPartition partition = new FastaInputPartition(
                path, CONTIG_NAME, null, new Configuration(),
                FastaInputPartition.DEFAULT_MAX_MERGE_GAP);

        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            assertTrue(reader.next());
            assertEquals(CONTIG_LEN, reader.get().getLong(2));
        }
    }

    @Test
    void localNio_indexedMode_nonExistentContig_throws() throws Exception {
        // IndexedFastaSequenceFile.getSequence() throws SAMException when the contig
        // is not in the index, propagating through next(). Verify this expected error path.
        String path = realn01Path();
        FastaInputPartition partition = new FastaInputPartition(
                path, "nonexistent_contig", null, new Configuration());

        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            assertThrows(Exception.class, reader::next,
                    "non-existent contig should throw from next()");
        }
    }

    @Test
    void close_beforeAnyNext_doesNotThrow() throws Exception {
        String path = realn01Path();
        FastaInputPartition partition = new FastaInputPartition(
                path, CONTIG_NAME, null, new Configuration());
        FastaPartitionReader reader = new FastaPartitionReader(partition);
        assertDoesNotThrow(reader::close);
    }

    @Test
    void faiEntry_accessorsExercised() throws Exception {
        Path fai = tempDir.resolve("acc.fa.fai");
        Files.writeString(fai, "c1\t300\t4\t50\t51\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry e = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "c1", new Configuration());

        assertEquals(300L, e.size);
        assertEquals(4L,   e.offset);
        assertEquals(50,   e.basesPerLine);
        assertEquals(51,   e.bytesPerLine);
    }

    // -------------------------------------------------------------------------
    // next() — Hadoop full-scan path via injected BufferedReader
    // -------------------------------------------------------------------------

    @Test
    void next_hadoopLineReader_fullScan_iteratesContigs() throws Exception {
        String fasta = ">chr1\nAAAA\n>chr2\nCCCC\n";
        FastaInputPartition partition = new FastaInputPartition(
                "dummy://host/file.fa", null, null, new Configuration());
        try (FastaPartitionReader reader = new FastaPartitionReader(
                partition, new BufferedReader(new StringReader(fasta)))) {
            assertTrue(reader.next());
            assertEquals("chr1", reader.get().getUTF8String(0).toString());
            assertTrue(reader.next());
            assertEquals("chr2", reader.get().getUTF8String(0).toString());
            assertFalse(reader.next());
        }
    }

    // -------------------------------------------------------------------------
    // readFaiEntry — number format error branch
    // -------------------------------------------------------------------------

    @Test
    void readFaiEntry_badNumbers_throwsIOException() throws Exception {
        Path fai = tempDir.resolve("badnums.fa.fai");
        // 5 fields but last field is non-numeric → NumberFormatException → IOException
        Files.writeString(fai, "chr1\t100\t6\t60\tNOTANUM\n", StandardCharsets.UTF_8);

        IOException ex = assertThrows(IOException.class, () ->
                FastaPartitionReader.readFaiEntry(
                        fai.toUri().toString(), "chr1", new Configuration()));
        assertTrue(ex.getMessage().toLowerCase().contains("malformed"));
    }

    // -------------------------------------------------------------------------
    // Hadoop indexed read — coalesced and per-line paths
    // -------------------------------------------------------------------------

    /** Opens a file using Hadoop LocalFileSystem and wraps it in HadoopSeekableStream. */
    private static HadoopSeekableStream openWithHadoopLocal(String pathStr,
                                                              Configuration conf)
            throws IOException {
        org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(pathStr);
        FileSystem fs = FileSystem.getLocal(conf);
        long len = fs.getFileStatus(hp).getLen();
        FSDataInputStream in = fs.open(hp);
        return new HadoopSeekableStream(in, len, pathStr);
    }

    @Test
    void hadoopStream_coalesced_readsContig() throws Exception {
        // newlineSize = bytesPerLine - basesPerLine = 61 - 60 = 1
        // DEFAULT_MAX_MERGE_GAP = 128 → 1 ≤ 128 → coalesced path
        String path = realn01Path();
        Configuration conf = new Configuration();
        HadoopSeekableStream stream = openWithHadoopLocal(path, conf);
        FastaPartitionReader.FaiEntry fai =
                new FastaPartitionReader.FaiEntry(CONTIG_LEN, FAI_OFFSET,
                                                  BASES_PER_LINE, BYTES_PER_LINE);
        FastaInputPartition partition = new FastaInputPartition(
                path, CONTIG_NAME, null, conf);

        try (FastaPartitionReader reader = new FastaPartitionReader(partition, stream, fai)) {
            assertTrue(reader.next());
            var row = reader.get();
            assertEquals(CONTIG_NAME, row.getUTF8String(0).toString());
            assertEquals(CONTIG_LEN,  row.getLong(2));
            assertTrue(row.getUTF8String(1).toString().startsWith(EXPECTED_PREFIX));
            assertFalse(reader.next()); // indexed mode: only one contig per partition
        }
    }

    @Test
    void hadoopStream_perLine_readsContig() throws Exception {
        // maxMergeGap = 0 → newlineSize(1) > 0 → per-line path
        String path = realn01Path();
        Configuration conf = new Configuration();
        HadoopSeekableStream stream = openWithHadoopLocal(path, conf);
        FastaPartitionReader.FaiEntry fai =
                new FastaPartitionReader.FaiEntry(CONTIG_LEN, FAI_OFFSET,
                                                  BASES_PER_LINE, BYTES_PER_LINE);
        FastaInputPartition partition = new FastaInputPartition(
                path, CONTIG_NAME, null, conf, 0);

        try (FastaPartitionReader reader = new FastaPartitionReader(partition, stream, fai)) {
            assertTrue(reader.next());
            var row = reader.get();
            assertEquals(CONTIG_NAME, row.getUTF8String(0).toString());
            assertEquals(CONTIG_LEN,  row.getLong(2));
            assertTrue(row.getUTF8String(1).toString().startsWith(EXPECTED_PREFIX));
            assertFalse(reader.next());
        }
    }

    @Test
    void hadoopStream_length_returnsFileSize() throws Exception {
        String path = realn01Path();
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(path);
        FileSystem fs = FileSystem.getLocal(conf);
        long expectedLen = fs.getFileStatus(hp).getLen();
        try (HadoopSeekableStream stream =
                new HadoopSeekableStream(fs.open(hp), expectedLen, path)) {
            assertEquals(expectedLen, stream.length());
        }
    }

    // -------------------------------------------------------------------------
    // isLocalPath — exception catch (path string not a valid URI)
    // -------------------------------------------------------------------------

    @Test
    void isLocalPath_pathWithSpaces_treatedAsLocal() {
        // URI.create() throws IllegalArgumentException for strings with unencoded spaces;
        // the catch block should treat such paths as local.
        assertTrue(FastaPartitionReader.isLocalPath("/some path/with spaces.fa"));
    }

    @Test
    void isLocalPath_bracesInPath_treatedAsLocal() {
        // '{' is an invalid URI character; URI.create throws → treated as local.
        assertTrue(FastaPartitionReader.isLocalPath("/{bad-uri}/file.fa"));
    }

    // -------------------------------------------------------------------------
    // toNioPath — exception catch (path string not a valid URI)
    // -------------------------------------------------------------------------

    @Test
    void toNioPath_pathWithSpaces_returnsNioPath() {
        // URI.create() throws for unencoded spaces; catch block falls back to Paths.get(pathStr).
        java.nio.file.Path p = FastaPartitionReader.toNioPath("/some path/with spaces.fa");
        assertNotNull(p);
        // The resulting path should contain the original string content
        assertTrue(p.toString().contains("spaces"));
    }

    @Test
    void toNioPath_bracesInPath_returnsNioPath() {
        java.nio.file.Path p = FastaPartitionReader.toNioPath("/{bad}/file.fa");
        assertNotNull(p);
    }

    // -------------------------------------------------------------------------
    // next() idempotency after exhaustion
    // -------------------------------------------------------------------------

    @Test
    void next_calledMultipleTimesAfterExhaustion_returnsFalse() throws Exception {
        String fasta = ">chr1\nACGT\n";
        FastaInputPartition partition = new FastaInputPartition(
                "dummy://host/file.fa", null, null, new Configuration());
        try (FastaPartitionReader reader = new FastaPartitionReader(
                partition, new BufferedReader(new StringReader(fasta)))) {
            assertTrue(reader.next());
            assertFalse(reader.next()); // first exhaustion
            assertFalse(reader.next()); // second call must not throw
        }
    }

    // -------------------------------------------------------------------------
    // FAI entry — single-base-per-line contig (basesPerLine = 1)
    // -------------------------------------------------------------------------

    @Test
    void faiEntry_singleBasePerLine_parsedCorrectly() throws Exception {
        // basesPerLine=1, bytesPerLine=2 (base + newline)
        Path fai = tempDir.resolve("single.fa.fai");
        Files.writeString(fai, "chrSingle\t4\t10\t1\t2\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry entry = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "chrSingle", new Configuration());

        assertEquals(4L, entry.size);
        assertEquals(10L, entry.offset);
        assertEquals(1, entry.basesPerLine);
        assertEquals(2, entry.bytesPerLine);
    }

    // -------------------------------------------------------------------------
    // Hadoop full-scan — contig with no trailing newline on last sequence line
    // -------------------------------------------------------------------------

    @Test
    void readNextContigViaHadoop_noTrailingNewline_returnsContig() throws Exception {
        // The last (only) sequence line has no trailing newline — still a valid read
        String fasta = ">chr1\nACGT";
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader(fasta)));

        ReferenceSequence seq = reader.readNextContigViaHadoop();
        assertNotNull(seq);
        assertEquals("chr1", seq.getName());
        assertEquals("ACGT", new String(seq.getBases(), StandardCharsets.US_ASCII));

        // EOF: next call must return null
        assertNull(reader.readNextContigViaHadoop());
    }

    // -------------------------------------------------------------------------
    // Hadoop full-scan — empty contig (header with no sequence lines)
    // -------------------------------------------------------------------------

    @Test
    void readNextContigViaHadoop_emptyContig_returnsEmptyBases() throws Exception {
        // A header immediately followed by another header — the first contig has 0 bases
        String fasta = ">empty\n>chr1\nACGT\n";
        FastaPartitionReader reader = readerWithLineReader(
                new BufferedReader(new StringReader(fasta)));

        ReferenceSequence first = reader.readNextContigViaHadoop();
        assertNotNull(first);
        assertEquals("empty", first.getName());
        assertEquals("", new String(first.getBases(), StandardCharsets.US_ASCII));

        ReferenceSequence second = reader.readNextContigViaHadoop();
        assertNotNull(second);
        assertEquals("chr1", second.getName());
    }

    // -------------------------------------------------------------------------
    // readFaiEntry — contig with zero-padded numeric fields
    // -------------------------------------------------------------------------

    @Test
    void readFaiEntry_firstContigInMultiContigFile_parsedCorrectly() throws Exception {
        // Validate that the first-match path in readFaiEntry works correctly when
        // the target contig is the very first line.
        Path fai = tempDir.resolve("first.fa.fai");
        Files.writeString(fai,
                "target\t500\t8\t80\t81\n" +
                "other\t200\t516\t80\t81\n",
                StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry entry = FastaPartitionReader.readFaiEntry(
                fai.toUri().toString(), "target", new Configuration());

        assertEquals(500L, entry.size);
        assertEquals(8L,   entry.offset);
        assertEquals(80,   entry.basesPerLine);
        assertEquals(81,   entry.bytesPerLine);
    }

    // -------------------------------------------------------------------------
    // readContigViaHadoop — "Unexpected EOF" IOException paths
    //
    // Both the coalesced path (newlineSize <= maxMergeGap) and per-line path
    // (newlineSize > maxMergeGap) throw IOException when the file is shorter
    // than the FAI entry declares.  We inject a HadoopSeekableStream over a
    // tiny FASTA and a fake FaiEntry that claims far more bases than exist.
    // -------------------------------------------------------------------------

    @Test
    void readContigViaHadoop_coalesced_truncatedFile_throwsIOException() throws Exception {
        // File has only 4 bases of sequence; FaiEntry claims 1000.
        // newlineSize = bytesPerLine(61) - basesPerLine(60) = 1
        // DEFAULT_MAX_MERGE_GAP = 128 → 1 <= 128 → coalesced path.
        // After reading the 4 real bytes, stream returns -1 → throws IOException.
        Path fa = tempDir.resolve("truncated_coalesced.fa");
        Files.writeString(fa, ">chr1\nACGT\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry fakeFai =
                new FastaPartitionReader.FaiEntry(1000L, 6L, 60, 61);
        Configuration conf = new Configuration();
        HadoopSeekableStream stream = openWithHadoopLocal(fa.toString(), conf);
        FastaInputPartition partition = new FastaInputPartition(
                fa.toString(), "chr1", null, conf);

        assertThrows(IOException.class, () -> {
            try (FastaPartitionReader reader =
                         new FastaPartitionReader(partition, stream, fakeFai)) {
                reader.next();
            }
        });
    }

    @Test
    void readContigViaHadoop_perLine_truncatedFile_throwsIOException() throws Exception {
        // maxMergeGap = 0 → newlineSize(1) > 0 → per-line path.
        // Partition with basesPerLine=60 tries to read 60 bytes but only 4 exist → -1 → throws.
        Path fa = tempDir.resolve("truncated_perline.fa");
        Files.writeString(fa, ">chr1\nACGT\n", StandardCharsets.UTF_8);

        FastaPartitionReader.FaiEntry fakeFai =
                new FastaPartitionReader.FaiEntry(1000L, 6L, 60, 61);
        Configuration conf = new Configuration();
        HadoopSeekableStream stream = openWithHadoopLocal(fa.toString(), conf);
        // maxMergeGap = 0 forces per-line reads
        FastaInputPartition partition = new FastaInputPartition(
                fa.toString(), "chr1", null, conf, 0);

        assertThrows(IOException.class, () -> {
            try (FastaPartitionReader reader =
                         new FastaPartitionReader(partition, stream, fakeFai)) {
                reader.next();
            }
        });
    }

    // -------------------------------------------------------------------------
    // HadoopSeekableStream — eof() returns true when stream is at end of file
    // -------------------------------------------------------------------------

    @Test
    void hadoopStream_eof_atEndOfFile_returnsTrue() throws Exception {
        String path = realn01Path();
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(path);
        FileSystem fs = FileSystem.getLocal(conf);
        long fileLen = fs.getFileStatus(hp).getLen();

        try (HadoopSeekableStream stream =
                new HadoopSeekableStream(fs.open(hp), fileLen, path)) {
            // Initially not at EOF
            assertFalse(stream.eof(), "stream should not be at EOF at position 0");
            // Seek to exact file length → position == length → eof() = true
            stream.seek(fileLen);
            assertTrue(stream.eof(), "stream should be at EOF after seeking to file length");
        }
    }

    // -------------------------------------------------------------------------
    // openWithHadoop — full-scan mode (contigName == null)
    //
    // Uses TestLocalFS mapped to scheme "raw" so that isLocalPath() returns
    // false (scheme != "file") while Hadoop can still read local files.
    // -------------------------------------------------------------------------

    /**
     * A minimal {@link RawLocalFileSystem} subclass that identifies itself with
     * the {@code raw} scheme instead of {@code file}.  This lets tests use
     * {@code raw:///absolute/path} URIs that pass through {@code isLocalPath}
     * (which only recognises {@code null} scheme and {@code file}) and reach
     * {@code openWithHadoop}, while Hadoop still reads from the real local
     * filesystem.
     */
    public static class TestLocalFS extends RawLocalFileSystem {
        private static final java.net.URI RAW_URI = java.net.URI.create("raw:///");

        @Override
        public java.net.URI getUri() {
            return RAW_URI;
        }
    }

    private static Configuration rawConf() {
        Configuration conf = new Configuration();
        conf.set("fs.raw.impl", TestLocalFS.class.getName());
        conf.setBoolean("fs.raw.impl.disable.cache", true);
        return conf;
    }

    /** Converts a local absolute path to a {@code raw:///} URI string. */
    private static String rawPath(String localAbsPath) {
        return "raw://" + localAbsPath;
    }

    @Test
    void openWithHadoop_fullScanMode_iteratesContig() throws Exception {
        // contigName=null, faiPath=null → openWithHadoop enters the else branch
        // (full-scan via BufferedReader) and open() takes the Hadoop path.
        String path = realn01Path();
        Configuration conf = rawConf();

        FastaInputPartition partition = new FastaInputPartition(
                rawPath(path), null, null, conf);

        int count = 0;
        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            while (reader.next()) {
                count++;
                assertNotNull(reader.get());
            }
        }
        // realn01.fa has exactly 1 contig
        assertEquals(1, count);
    }

    @Test
    void openWithHadoop_indexedMode_readsContig() throws Exception {
        // contigName != null && faiPath != null → openWithHadoop enters the if branch
        // (indexed mode: HadoopSeekableStream + FaiEntry lookup).
        String path = realn01Path();
        // Derive the FAI path from the FASTA path (realn01.fa.fai is co-located)
        String faiPath = path + ".fai";
        Configuration conf = rawConf();

        FastaInputPartition partition = new FastaInputPartition(
                rawPath(path), CONTIG_NAME, rawPath(faiPath), conf);

        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            assertTrue(reader.next());
            var row = reader.get();
            assertEquals(CONTIG_NAME, row.getUTF8String(0).toString());
            assertEquals(CONTIG_LEN,  row.getLong(2));
            assertFalse(reader.next()); // indexed mode: only one contig per partition
        }
    }

    @Test
    void openWithHadoop_contigNameSetFaiPathNull_opensHadoopElseBranch() throws Exception {
        // contigName != null but faiPath == null → the && short-circuits to else in
        // openWithHadoop (full-scan via BufferedReader).  next() subsequently throws
        // IllegalStateException because contigName != null but neither hadoopStream
        // nor refFile was set — this is an invalid/unreachable configuration in
        // production (FastaScan never creates such a partition), but it covers the
        // `faiPath == null` branch of the && guard in openWithHadoop.
        String path = realn01Path();
        Configuration conf = rawConf();

        FastaInputPartition partition = new FastaInputPartition(
                rawPath(path), CONTIG_NAME, null, conf);

        try (FastaPartitionReader reader = new FastaPartitionReader(partition)) {
            assertThrows(IllegalStateException.class, reader::next);
        }
    }
}
