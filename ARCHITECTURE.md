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
   BamScanBuilder         ← SupportsPushDownV2Filters + SupportsPushDownLimit
          │               + SupportsPushDownRequiredColumns
          │  pushes referenceName equality; extracts start range for BAI query
          ▼
       BamScan            ← Scan + Batch + SupportsReportStatistics
          │               + SupportsReportOrdering
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
│   src/main/java/io/github/peterdowdy/litebfx/
│   │   HadoopSeekableStream.java
│   │   SerializableConfiguration.java
│   │   bam/   vcf/   fastq/   fasta/   bed/
│   src/test/java/         ← standard unit + integration tests
│   src/test-s3/           ← compiled only with -Ps3-integration
│   src/test-gcs/          ← compiled only with -Pgcs-integration
│   src/test-azure/        ← compiled only with -Pazure-integration
scala/                     ← Scala API fat JAR (embeds core via shade merge)
    src/main/scala/io/github/peterdowdy/litebfx/scala/
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

### ScanBuilder: predicate pushdown, limit, and column pruning

`ScanBuilder` is the only place Spark communicates query intent to the library before reading starts. Each format's builder implements a subset of the following interfaces.

**V2 predicate pushdown** (`SupportsPushDownV2Filters`): The builder receives a flat array of V2 `Predicate` objects (after `And`-tree flattening). Recognized predicates are split into two categories:

- **Fully pushed** (returned from `pushedPredicates()`): Equality predicates on the reference-name column (`referenceName`, `chrom`, `name`). The index query for a given reference guarantees an exact match, so Spark trusts the scan and removes the predicate from the physical plan — it never appears in a post-scan `Filter` node.
- **Extracted but not pushed** (returned as unhandled from `pushPredicates()`): Range predicates on the coordinate column (`start`, `pos`, `chromStart`, `chromEnd`). Their values are stored and used to construct a narrower BAI/tabix query, reducing I/O. However, because BAI and tabix use overlap queries that may return records whose alignment starts outside the exact range, these predicates are returned as unhandled so Spark adds a post-scan `Filter` node for correctness.

| Format | Pushed (exact) | Extracted for index, post-filtered by Spark |
|---|---|---|
| BAM/CRAM | `referenceName =` | `start >=`, `start >`, `start <=`, `start <` |
| VCF/BCF | `chrom =` | `pos >=`, `pos >`, `pos <=`, `pos <` |
| BED | `chrom =` | `chromStart >=`, `chromStart >`, `chromEnd <=`, `chromEnd <` |
| FASTA | `name =` | _(none — FAI lookup is exact)_ |
| FASTQ | _(none — no index)_ | — |

Helper methods (`flatten`, `isColumnEquality`, `isRangeComparison`, `literalValue`, `columnName`) live in `FastaScanBuilder` as `public static` utilities reused by all other builders.

**Limit pushdown** (`SupportsPushDownLimit`): All formats implement `pushLimit(int limit)`. The pushed limit is passed to the `Scan`, which uses it to plan fewer partitions and cap the per-partition row count. This makes `df.limit(N)` avoid reading the entire file when only the first N rows are needed. The method returns `false` so Spark also enforces the limit post-scan.

**Column pruning** (`SupportsPushDownRequiredColumns.pruneColumns`): The builder records the required schema. For BAM, if `attributes` is absent the reader skips per-record SAM tag parsing — a meaningful speedup for aggregation queries that don't need optional tags.

### Scan: partition planning, statistics, and ordering

`Scan.planInputPartitions()` runs on the **driver** and returns a serializable array of partition descriptors. The driver opens indexes (BAI, tabix, FAI) here so executors never parse index files independently.

See the [Partition planning](#partition-planning) section for the full decision trees.

**Statistics** (`SupportsReportStatistics`): All five format scans implement `estimateStatistics()`. The driver resolves files and sums their sizes to produce a `sizeInBytes` estimate, which the Spark optimizer uses for join reordering and broadcast decisions. `FastaScan` also reports `numRows` as the FAI contig count when an index is present, enabling accurate cardinality estimates for FASTA queries.

**Ordering** (`SupportsReportOrdering`): Three formats report output ordering when an appropriate index confirms sorted data:

| Format | Condition | Reported order |
|---|---|---|
| BAM/CRAM | BAI/CRAI present **and** `SO:coordinate` in header | `[referenceName ASC NULLS LAST, start ASC NULLS LAST]` |
| VCF/BCF | tabix index present | `[chrom ASC NULLS LAST, pos ASC NULLS LAST]` |
| BED | tabix index present | `[chrom ASC NULLS LAST, chromStart ASC NULLS LAST]` |

When ordering is reported, the Spark optimizer can eliminate sort operations for queries like `ORDER BY chrom, pos` or merge-joins on genomic coordinates without adding a shuffle.

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

The planner chooses among several partition modes in priority order (`useIndex=false`
simply forces `indexPath` to null, routing into the no-index branches below):

```
SAM (any path)
    └─ line-level byte-range splits                      (planSamSplitPartitions)

BAM, BAI found, no pushed region
    └─ VFO-based per-reference partitions + unmapped     (planVfoPartitions)

BAM, no BAI, no pushed region
    └─ BGZF block-level byte-range splits                (planBgzfSplitPartitions)

CRAM, no pushed region
    ├─ CRAI found  →  CRAI container-level partitions     (planCraiPartitions)
    └─ no CRAI     →  header-scan container partitions    (planCramContainerSplitPartitions)

BAM/CRAM, region pushed
    ├─ index found →  single forRegionQuery partition
    └─ no index    →  single forFullScan partition
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
| `htsjdk.*` | `io.github.peterdowdy.litebfx.shaded.htsjdk.*` |
| `com.google.common.*` (Guava) | `io.github.peterdowdy.litebfx.shaded.guava.*` |

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
2. `pushPredicates([=(referenceName,"chr1"), >=(start,1000000), <=(start,2000000)])`:
   - `referenceName =` equality → **pushed** (stored as `pushedReferenceName="chr1"`; absent from physical plan)
   - `start >=` / `start <=` range → **extracted** (stored as `pushedStart`, `pushedEnd`) but returned unhandled → Spark adds post-scan `Filter` nodes for these
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
   - `samReader.query("chr1", 1000000, 2000000, false)` — htsjdk seeks via BAI VFOs; may return reads overlapping the boundary
9. For each record from htsjdk iterator:
   - `next()` advances iterator; if `rowsRead >= rowLimit` returns false immediately
   - `get()` converts `SAMRecord` → `GenericInternalRow(13 values)` — `attributes` skipped (pruned)
10. Spark applies post-scan `Filter` for `start >= 1000000 AND start <= 2000000` (correctness pass for overlap reads), projects `{readName, start, cigar}`, counts

Only BGZF blocks containing chr1:1000000–2000000 are fetched from S3 — not the full file.
