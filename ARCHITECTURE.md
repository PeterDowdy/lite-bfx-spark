# Architecture

## Overview

lite-bfx-spark implements Apache Spark 4.x **DataSource V2** readers for genomics file formats. Every format follows the same structural pattern — a six-layer chain that Spark drives at query time — with format-specific partition planning, index resolution, and record decoding layered underneath.

```
spark.read.format("bam").load(...)
          │
          ▼
   BamDataSource          ← TableProvider + DataSourceRegister ("bam")
          │
          ▼
      BamTable            ← SupportsRead
          │
          ▼
   BamScanBuilder         ← SupportsPushDownFilters + SupportsPushDownRequiredColumns
          │  extracts referenceName / start filters, tracks attribute pruning
          ▼
       BamScan            ← Scan + Batch
          │  planInputPartitions(): produces BamInputPartition[]
          ▼
BamPartitionReaderFactory ← PartitionReaderFactory
          │
          ▼
  BamPartitionReader      ← PartitionReader<InternalRow>
             next() / get() convert SAMRecord → Spark InternalRow
```

The same chain exists for every format: `{Vcf,Fastq,Fasta,Bed}{DataSource,Table,ScanBuilder,Scan,InputPartition,PartitionReaderFactory,PartitionReader,Schema}`. `CramDataSource` is a thin shell that delegates to the entire BAM chain with `isCram=true`.

---

## Module layout

```
pom.xml                    ← parent; Spark profile sets spark/hadoop/scala versions
core/                      ← Java-only library JAR (published to Maven Central)
│   pom.xml                ← shades htsjdk + Guava; three cloud integration profiles
│   src/main/java/com/litebfx/
│   │   HadoopSeekableStream.java
│   │   SerializableConfiguration.java
│   │   bam/   vcf/   fastq/   fasta/   bed/
│   src/test/java/         ← standard unit + integration tests
│   src/test-s3/           ← compiled only with -Ps3-integration
│   src/test-gcs/          ← compiled only with -Pgcs-integration
│   src/test-azure/        ← compiled only with -Pazure-integration
scala/                     ← Scala API fat JAR (embeds core via shade merge)
    src/main/scala/com/litebfx/scala/
        package.scala          ← implicits wiring
        GenomicRegion.scala    ← typed interval value object
        DataFrameReaderOps.scala
        LiteBfxSpark.scala     ← non-implicit entry point
```

### Build profiles

| Profile | Spark | Hadoop | Scala |
|---|---|---|---|
| `spark402` (default) | 4.0.2 | 3.4.1 | 2.13.x |
| `spark411` | 4.1.1 | 3.4.2 | 2.13.x |

Cloud integration profiles (`s3-integration`, `gcs-integration`, `azure-integration`) compile additional test sources and activate cloud-specific Maven Surefire configuration.

---

## DataSource V2 chain

### Entry point registration

Each format registers itself via the Java service-loader mechanism:

```
core/src/main/resources/META-INF/services/
    org.apache.spark.sql.sources.DataSourceRegister
```

This file lists all `DataSource` class names. Spark discovers them at runtime through `DataSourceRegister.shortName()`, which is how `spark.read.format("bam")` resolves to `BamDataSource`.

### ScanBuilder: filter pushdown and column pruning

`ScanBuilder` is the only place Spark communicates query intent to the library before reading starts.

**Filter pushdown** (`SupportsPushDownFilters.pushFilters`): The builder inspects the filter array for recognizable predicates and stores them for use in partition planning. All filters are returned as "unhandled" — Spark re-applies every filter post-scan as a correctness guarantee.

| Format | Pushable filters | Column |
|---|---|---|
| BAM/CRAM | Equality + range | `referenceName`, `start` |
| VCF/BCF | Equality + range | `chrom`, `pos` |
| BED | Equality + range | `chrom`, `chromStart`, `chromEnd` |
| FASTA | Equality | `name` |
| FASTQ | _(none — no index)_ | — |

**Column pruning** (`SupportsPushDownRequiredColumns.pruneColumns`): The builder records the required schema. For BAM, if `attributes` is absent the reader skips per-record SAM tag parsing — a meaningful speedup for aggregation queries that don't need optional tags.

### Scan: partition planning

`Scan.planInputPartitions()` runs on the **driver** and returns a serializable array of partition descriptors. The driver opens indexes (BAI, tabix, FAI) here so executors never parse index files independently.

See the [Partition planning](#partition-planning) section for the full decision trees.

### PartitionReader: record conversion

Each executor creates a `PartitionReader` per partition. The reader:

1. Opens the file through Hadoop `FileSystem` (`fs.open(path)`)
2. Wraps the `FSDataInputStream` in `HadoopSeekableStream`
3. Passes the seekable stream to htsjdk (`SamReaderFactory`, `VCFFileReader`, etc.)
4. Iterates records via `next()`, converting each to a Spark `InternalRow` in `get()`

Format-specific fields are mapped to Spark types once per record; no intermediate Java objects are created.

---

## Partition planning

### BAM/CRAM — `BamScan.planInputPartitions()`

The planner chooses among six partition modes in priority order:

```
indexPath found AND no pushed region
    ├─ BAM/SAM  →  VFO-based per-reference partitions  (planVfoPartitions)
    └─ CRAM     →  CRAI container-level partitions       (planCraiPartitions)
                   or header-scan partitions if no CRAI  (planCramContainerSplitPartitions)

indexPath found AND region pushed
    └─ any      →  single forRegionQuery partition

indexPath NOT found AND no pushed region
    ├─ BAM      →  BGZF block-level byte-range splits    (planBgzfSplitPartitions)
    └─ SAM      →  line-level byte-range splits           (planSamSplitPartitions)

useIndex=false
    └─ any      →  single forFullScan partition
```

**VFO-based splitting** (BAM + BAI, most common production case):

The driver reads the BAM header and BAI index once, enumerates all reference sequences, and groups them into ≤ `numPartitions` (default 200) groups. Each group becomes one partition whose executor calls `samReader.query(intervals, false)` — htsjdk internally uses the BAI's virtual file offsets (VFOs) to seek precisely to each reference's BGZF chunks, skipping all other chromosomes.

An additional unmapped partition is always appended so unplaced reads are not lost.

**BGZF block-level splitting** (BAM, no BAI):

The file is divided into chunks of `bgzfSplitSize` bytes (default 128 MB). Chunk boundaries are arithmetic (`i × splitSize`), not BGZF-block-aligned — computing real BGZF boundaries would require a full sequential driver-side scan of potentially thousands of blocks on object storage. Instead, each executor scans forward ≤ 65 KB from its arithmetic boundary to find the next BGZF magic bytes (`1f 8b 08 04`) and then the next clean BAM record start. Chunks that contain no clean record start emit zero rows safely.

**CRAI-based CRAM splitting**:

The driver parses the gzip-compressed CRAI file (a TSV of alignment slice metadata), extracts unique container byte offsets via `TreeSet<Long>`, and distributes them across ≤ `numPartitions` groups. Each group stores a pair of VFO-encoded span boundaries; the executor reads containers spanning that range with `CRAMIterator`.

### VCF/BCF — `VcfScan.planInputPartitions()`

```
bgzipped + tabix found AND no pushed region
    └─  one partition per chromosome group (groupChroms, ≤ numPartitions)

bgzipped + tabix found AND chrom/pos pushed
    └─  single region-query partition

plain-text VCF
    └─  byte-range splits (vcfSplitSize, default 128 MB)

no index
    └─  single full-scan partition
```

For tabix-indexed VCF/BCF, the driver reads the tabix index on the driver to enumerate chromosomes, then groups them so no executor reads more than its fair share. Each executor calls `VCFFileReader.query(chrom, start, end)` for each chromosome in its group, using tabix to seek directly to the relevant BGZF blocks.

For local paths, `VCFFileReader` handles the query. For cloud URIs, a custom BGZF-over-Hadoop path decompresses the file via `BlockCompressedInputStream(HadoopSeekableStream)` and applies the tabix index manually.

### FASTQ — `FastqScan.planInputPartitions()`

```
BGZF-compressed (.fastq.gz with BGZF framing)
    └─  BGZF block-level splits (bgzfSplitSize, default 128 MB)

plain-gzip (.fastq.gz without BGZF framing)
    └─  single partition (gzip is not seekable)

uncompressed
    └─  byte-range splits: min(numPartitions, floor(fileSize / minSplitBytes))
        minSplitBytes default = 64 MB
```

FASTQ has no index format, so there is no push-down optimization — the number of partitions controls parallelism only.

### FASTA — `FastaScan.planInputPartitions()`

```
FAI index found AND name filter pushed
    └─  single partition for the matching contig

FAI index found AND no filter
    └─  one partition per contig (random-access via FAI byte offsets)

no FAI
    └─  single full-scan partition
```

The FAI format (five tab-separated columns: NAME, LENGTH, OFFSET, BASES_PER_LINE, BYTES_PER_LINE) gives the exact byte offset and line geometry for every contig, so each executor can seek directly to its contig's data without reading any other part of the file.

### BED — `BedScan.planInputPartitions()`

```
bgzipped + tabix found AND chrom/pos pushed
    └─  single region-query partition

bgzipped + tabix found AND no pushed region
    └─  single full-scan partition

plain-text BED
    └─  byte-range splits (bedSplitSize, default 128 MB)

no index
    └─  single full-scan partition
```

BED does not do per-chromosome partition grouping (unlike VCF). This is a known gap: unfiltered reads on a large bgzipped BED always produce one partition.

---

## Index resolution

All index lookups go through Hadoop `FileSystem` so cloud URIs work identically to local paths.

### BAM/CRAM — `BamScan.resolveIndexPath()`

1. `indexPath` option (exact path, single-file reads only)
2. `<indexDir>/<filename>.bai` (shared index directory)
3. `<bamPath>.bai` or `<cramPath>.cram.crai` (co-located)
4. `null` → full scan / BGZF split fallback

### VCF/BCF and BED — `VcfScan.resolveIndexPath()` / `BedScan`

1. `indexPath` option
2. `<path>.tbi` (tabix)
3. `<path>.csi` (CSI — columnar index, supports larger chromosomes)
4. `null` → full scan

### FASTA — `FastaScan.resolveFaiPath()`

1. `indexPath` option
2. `<fastaPath>.fai` (co-located)
3. `null` → full scan

---

## Cloud I/O: HadoopSeekableStream and SerializableConfiguration

### HadoopSeekableStream

htsjdk expects a `SeekableStream` — an `InputStream` extended with `seek(long)`, `position()`, `length()`, and `eof()`. Hadoop's `FSDataInputStream` is seekable but does not implement `SeekableStream`. `HadoopSeekableStream` is the adapter:

```java
public class HadoopSeekableStream extends SeekableStream {
    private final FSDataInputStream in;   // S3A / ADLS / GCS / local / HDFS …
    private final long length;

    public void seek(long position) throws IOException { in.seek(position); }
    public long position() throws IOException          { return in.getPos(); }
    public boolean eof() throws IOException            { return in.getPos() >= length; }
    // … read(), close(), getSource()
}
```

With this single adapter, every htsjdk reader (BAM, CRAM, VCF, FASTQ, FASTA, tabix) works on any Hadoop-compatible storage without format-specific cloud code. The S3A connection behavior for random-access workloads is configured in tests with `fs.s3a.input.fadvise=random` to prevent keepalive-connection draining from inflating byte counts.

### SerializableConfiguration

Partition descriptors (`BamInputPartition`, `VcfInputPartition`, etc.) are serialized by Spark's driver and deserialized by each executor. They carry a `SerializableConfiguration` wrapping the Hadoop `Configuration` that contains cloud credentials, endpoints, and tuning parameters.

`Configuration` holds a non-serializable `ClassLoader`. The wrapper serializes only the key-value property map using `Configuration`'s own `Writable` protocol:

```java
// Serialize (driver side)
private void writeObject(ObjectOutputStream out) {
    out.defaultWriteObject();   // no-op: value is transient
    value.write(out);           // Writable: writes all k-v pairs
}

// Deserialize (executor side)
private void readObject(ObjectInputStream in) {
    in.defaultReadObject();
    value = new Configuration(false);   // no default resources loaded
    value.readFields(in);               // restore k-v pairs
}
```

This propagates S3A endpoints, IAM credentials, ADLS service principals, and GCS project IDs from the driver to all executors automatically, without any per-format credential-passing code.

---

## Shading strategy

`core/pom.xml` uses `maven-shade-plugin` to relocate two dependency namespaces:

| Original | Relocated to |
|---|---|
| `htsjdk.*` | `com.litebfx.shaded.htsjdk.*` |
| `com.google.common.*` (Guava) | `com.litebfx.shaded.guava.*` |

Spark and Hadoop jars are excluded from the shade (they are `provided`-scope dependencies).

**Why shade htsjdk?** htsjdk 4.x bundles Guava. Spark and Databricks bundle different Guava versions. Without relocation, whichever Guava loads first wins, causing `NoSuchMethodError` or `ClassCastException` at runtime. Relocating htsjdk's Guava usage into a private namespace eliminates the conflict entirely.

**Why no Disq or hadoop-bam?** Both libraries are unmaintained and architecturally tied to Spark 3.x. More critically, hadoop-bam's transitive dependency chain brings its own htsjdk version that conflicts with the one we shade, and Disq has proven incompatible with the Databricks classloader isolation model. Implementing BAI-based partition planning directly on top of htsjdk requires more code but produces a cleaner runtime artifact.

The `scala/pom.xml` shade also merges `META-INF/services` entries from core via `ServicesResourceTransformer`, so Spark's service-loader can discover the `DataSourceRegister` implementations inside the combined Scala fat JAR.

---

## Scala API

The `scala` module adds a thin idiomatic layer on top of the Java core without re-implementing any I/O logic.

### `GenomicRegion`

An immutable value object for 1-based inclusive genomic intervals:

```scala
final case class GenomicRegion(chromosome: String, start: Int, end: Int)
```

Validation runs at construction (`start > 0`, `end >= start`). Companion factory methods cover common patterns: `GenomicRegion("chr1", 100)` for a single locus; `GenomicRegion.wholeChromosome("chrX")` for full-chromosome queries.

### `DataFrameReaderOps`

An implicit class on `DataFrameReader` that translates named Scala parameters to DataSource V2 option strings:

```scala
spark.read.bam("s3a://bucket/cohort/", indexDir = Some("s3a://idx/"))
// equivalent to:
spark.read.format("bam").option("indexDir", "s3a://idx/").load("s3a://bucket/cohort/")
```

### `implicits` object and `LiteBfxSpark`

`package.scala` exports a single `implicits` object with one implicit conversion (`DataFrameReader → DataFrameReaderOps`). All DataFrame-level filtering uses standard Spark `.filter()` and `.drop()` directly — predicate pushdown to BAI/tabix is triggered automatically by the scan builder. `LiteBfxSpark` provides the same reader API as explicit method calls for users who prefer to avoid implicit imports.

---

## Data flow: end-to-end example

Reading a BAM region on S3 with a `referenceName` filter:

```
spark.read.format("bam")
  .option("indexPath", "s3a://idx/sample.bam.bai")
  .load("s3a://bucket/sample.bam")
  .filter("referenceName = 'chr1' AND start >= 1000000 AND start <= 2000000")
  .select("readName", "start", "cigar")
  .count()
```

**Driver:**
1. Spark instantiates `BamDataSource` → `BamTable` → `BamScanBuilder`
2. `pushFilters([EqualTo("referenceName","chr1"), GTE("start",1000000), LTE("start",2000000)])`:
   stores `pushedReferenceName="chr1"`, `pushedStart=1000000`, `pushedEnd=2000000`; returns all filters unhandled
3. `pruneColumns({readName, start, cigar})`: sets `includeAttributes=false`
4. `build()` → `BamScan(…, "chr1", 1000000, 2000000, isCram=false)`
5. `BamScan.planInputPartitions()`:
   - Opens BAM header + BAI via S3A
   - `pushedReferenceName != null` → single `forRegionQuery` partition
   - Partition serialized with `SerializableConfiguration` containing S3A credentials
6. `createReaderFactory()` → `BamPartitionReaderFactory`

**Executor:**
7. `BamPartitionReaderFactory.createReader(partition)` → `BamPartitionReader`
8. `open()`:
   - `fs.open("s3a://bucket/sample.bam")` → `FSDataInputStream`
   - Wrapped in `HadoopSeekableStream`
   - BAI opened the same way → second `HadoopSeekableStream`
   - `SamReaderFactory.open(SamInputResource.of(bamStream).index(baiStream))`
   - `samReader.query("chr1", 1000000, 2000000, false)` — htsjdk seeks via BAI VFOs
9. For each record from htsjdk iterator:
   - `next()` advances iterator
   - `get()` converts `SAMRecord` → `GenericInternalRow(13 values)` — `attributes` skipped (pruned)
10. Spark applies post-filter (correctness pass), projects `{readName, start, cigar}`, counts

Only BGZF blocks containing chr1:1000000–2000000 are fetched from S3 — not the full file.
