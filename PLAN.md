# lite-bfx-spark: Implementation Plan

## Context

Build a new Java library from scratch that exposes genomics file formats as first-class Apache Spark DataFrame data sources, compatible with Spark 4.x and Databricks Runtime. Supported formats:

| Format | Extensions | Index | Spark short name |
|---|---|---|---|
| BAM | `.bam` | `.bai` | `bam` |
| SAM | `.sam` | _(none)_ | `bam` (auto-detected) |
| CRAM | `.cram` | `.cram.crai` | `cram` |
| FASTQ | `.fastq`, `.fq`, `.fastq.gz`, `.fq.gz` | _(none)_ | `fastq` |
| VCF / BCF | `.vcf`, `.vcf.gz`, `.bcf` | `.tbi`, `.csi` | `vcf` |
| FASTA | `.fa`, `.fasta` | `.fai` | `fasta` |
| BED | `.bed`, `.bed.gz` | `.tbi`, `.csi` | `bed` |

The goal is "samtools/bcftools/seqkit-like" access to genomics records — all standard fields — but parallelized across a Spark cluster, with cloud-storage support (S3/ADLS/GCS via Hadoop FileSystem).

---

## Technology Decisions

| Concern | Choice | Reason |
|---|---|---|
| Language | Java 17 | Required for Spark 4.0; no Scala binary-version matrix concerns |
| Build | Maven + shade plugin | Bundles HTSJDK; avoids classpath conflicts on Databricks |
| BAM/SAM/CRAM parsing | htsjdk (latest stable) | Industry standard; `SamReaderFactory` auto-detects all three formats |
| CRAM splitting | CRAI index → `CRAIEntry` virtual offsets | Analogous to BAI chunking; no extra dependencies |
| BAM splitting | BAI index → virtual file offsets | No extra dependencies; production BAM files always have BAI |
| FASTQ parsing | htsjdk `FastqReader` | Handles `.fastq`/`.fq`; line-count splitting for uncompressed files |
| VCF/BCF parsing | htsjdk `VCFFileReader` (Tribble) | Handles `.vcf`, `.vcf.gz`, `.bcf`; tabix/CSI index support |
| VCF/BED splitting | Tabix (`.tbi`) / CSI (`.csi`) → virtual offsets | Same HTSJDK `TabixIndex` API for both |
| FASTA parsing | htsjdk `IndexedFastaSequenceFile` | `.fai` index enables one-partition-per-contig |
| BED parsing | htsjdk `BEDCodec` via Tribble `AbstractFeatureReader` | Reuses tabix infrastructure shared with VCF |
| Spark API | DataSource V2 (Spark 4.x) | Proper partitioning, predicate pushdown, column pruning |
| Fallback | Single partition when no index found | Graceful degradation for unindexed files |
| Scala wrapper | `lite-bfx-spark-scala_2.13` (separate Maven module) | Spark-idiomatic API (implicits, `GenomicRegion`, extension methods) without coupling Java core to Scala binary version |

**Disq is NOT used** — it adds hadoop-bam as a transitive dependency and creates version conflicts on Databricks. HTSJDK alone is sufficient when we implement index-based splitting ourselves.

**SAM files are supported** — HTSJDK's `SamReaderFactory` auto-detects SAM vs BAM vs CRAM from file content (magic bytes). SAM files always get a single partition (no BGZF, no index possible).

---

## Project Structure

```
lite-bfx-spark/
├── pom.xml                                   ← parent POM (packaging=pom)
├── core/                                     ← Java module
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/com/litebfx/
│       │   │   ├── BamDataSource.java           ← TableProvider + DataSourceRegister (BAM/SAM)
│       │   │   ├── BamTable.java
│       │   │   ├── BamScanBuilder.java
│       │   │   ├── BamScan.java
│       │   │   ├── BamInputPartition.java
│       │   │   ├── BamPartitionReaderFactory.java
│       │   │   ├── BamPartitionReader.java
│       │   │   ├── HadoopSeekableStream.java
│       │   │   ├── BamSchema.java
│       │   │   ├── SerializableConfiguration.java
│       │   │   ├── cram/
│       │   │   │   └── CramDataSource.java      ← registers "cram"; delegates to Bam* with isCram=true
│       │   │   ├── fastq/
│       │   │   │   ├── FastqDataSource.java     ← registers "fastq"
│       │   │   │   ├── FastqTable.java
│       │   │   │   ├── FastqSchema.java
│       │   │   │   ├── FastqScan.java           ← line-count splitting for uncompressed
│       │   │   │   ├── FastqInputPartition.java
│       │   │   │   ├── FastqPartitionReaderFactory.java
│       │   │   │   └── FastqPartitionReader.java
│       │   │   ├── vcf/
│       │   │   │   ├── VcfDataSource.java       ← registers "vcf"
│       │   │   │   ├── VcfTable.java
│       │   │   │   ├── VcfSchema.java
│       │   │   │   ├── VcfScanBuilder.java      ← tabix predicate pushdown
│       │   │   │   ├── VcfScan.java
│       │   │   │   ├── VcfInputPartition.java
│       │   │   │   ├── VcfPartitionReaderFactory.java
│       │   │   │   └── VcfPartitionReader.java
│       │   │   ├── fasta/
│       │   │   │   ├── FastaDataSource.java     ← registers "fasta"
│       │   │   │   ├── FastaTable.java
│       │   │   │   ├── FastaSchema.java
│       │   │   │   ├── FastaScan.java           ← one partition per contig (via .fai)
│       │   │   │   ├── FastaInputPartition.java
│       │   │   │   ├── FastaPartitionReaderFactory.java
│       │   │   │   └── FastaPartitionReader.java
│       │   │   └── bed/
│       │   │       ├── BedDataSource.java       ← registers "bed"
│       │   │       ├── BedTable.java
│       │   │       ├── BedSchema.java
│       │   │       ├── BedScanBuilder.java      ← tabix predicate pushdown
│       │   │       ├── BedScan.java
│       │   │       ├── BedInputPartition.java
│       │   │       ├── BedPartitionReaderFactory.java
│       │   │       └── BedPartitionReader.java
│       │   └── resources/META-INF/services/
│       │       └── org.apache.spark.sql.sources.DataSourceRegister
│       └── test/
│           └── java/com/litebfx/
│               └── BamDataSourceTest.java
├── scala/                                    ← Scala wrapper module
│   ├── pom.xml
│   └── src/
│       ├── main/scala/com/litebfx/scala/
│       │   ├── package.scala                ← implicits object
│       │   ├── GenomicRegion.scala          ← value type for genomic intervals
│       │   ├── DataFrameReaderOps.scala     ← spark.read.bam(...)
│       │   ├── DataFrameOps.scala           ← df.filterRegion(...)
│       │   └── LiteBfxSpark.scala              ← explicit (non-implicit) entry point
│       └── test/scala/com/litebfx/scala/
│           ├── DataFrameReaderOpsTest.scala
│           ├── DataFrameOpsTest.scala
│           └── LiteBfxSparkTest.scala
└── tests/smoke/                              ← unchanged (independent Maven project)
```

---

## Scala Wrapper Module

The `scala/` submodule publishes `lite-bfx-spark-scala_2.13` — a fat JAR that includes the Java core and adds a Spark-idiomatic Scala API.

### Build overview

Three Maven POMs in a reactor:

- **Root `pom.xml`** — `packaging=pom`, modules `[core, scala]`. All versions in `<properties>` (`spark.version=4.0.2`, `scala.version=2.13.16`, `scala.binary.version=2.13`). `<dependencyManagement>` and `<pluginManagement>` for all children.
- **`core/pom.xml`** — Java-only. shade plugin relocates htsjdk. Published as `lite-bfx-spark`.
- **`scala/pom.xml`** — `scala-maven-plugin 4.9.2` compiles Scala. shade plugin merges core JAR into the scala fat JAR (single-JAR install for cluster users). Published as `lite-bfx-spark-scala_2.13`.

**Why fat-shade the scala module:** Users install one JAR and get the full Scala API plus the DataSource V2 implementation. The `DataSourceRegister` service file from core is included automatically, so `format("bam")` works without loading core separately. Core is still published standalone for Java/Python users.

**artifactId convention:** `lite-bfx-spark-scala_2.13` follows the standard Scala ecosystem convention (Spark, Akka, Kafka all use this). Enables `%%` in sbt. The `_2.13` suffix is a naming convention only — Maven doesn't interpret it.

### Scala API

```scala
import com.litebfx.scala.implicits._    // single import; matches spark.implicits._ pattern
import com.litebfx.scala.GenomicRegion

// BAM — read whole file
val df = spark.read.bam("s3a://bucket/sample.bam")

// BAM — with options
val df = spark.read.bam(
  "s3a://bucket/sample.bam",
  indexPath = Some("s3a://idx/sample.bam.bai"),
  numPartitions = 100
)

// CRAM — referenceFile required for sequence decoding
val df = spark.read.cram(
  "s3a://bucket/sample.cram",
  referenceFile = Some("s3a://ref/hg38.fa")
)

// BAM — predicate pushdown via extension methods
val reads = spark.read
  .bam("s3a://data/cohort/", indexDir = Some("s3a://idx/cohort/"))
  .filterRegion("chr17", 43044295, 43125370)   // pushdown via BAI
  .filterMappingQuality(30)                     // row-level filter

// FASTQ
val fq = spark.read.fastq("s3a://bucket/reads.fastq.gz")

// VCF — with tabix pushdown
val variants = spark.read
  .vcf("s3a://bucket/calls.vcf.gz", indexPath = Some("s3a://bucket/calls.vcf.gz.tbi"))
  .filterVariantRegion("chr1", 1000000, 2000000)

// FASTA — one row per contig
val ref = spark.read.fasta("s3a://ref/hg38.fa")

// BED — with tabix pushdown
val peaks = spark.read
  .bed("s3a://bucket/peaks.bed.gz")
  .filterBedRegion("chr1", 0, 1000000)

// GenomicRegion as a typed value object (1-based inclusive, matches BAM/VCF convention)
val brca1 = GenomicRegion("chr17", 43044295, 43125370)
df.filterRegion(brca1)

// Non-implicit API (no import needed for implicits)
val df = LiteBfxSpark.readRegion(spark, "s3a://...", LiteBfxSpark.region("chr1", 1, 10000))
```

### Package layout

| File | Contents |
|---|---|
| `package.scala` | `object implicits` — single opt-in import wiring all implicit classes |
| `GenomicRegion.scala` | `case class GenomicRegion(chromosome, start, end)` + companion helpers |
| `DataFrameReaderOps.scala` | `implicit class` on `DataFrameReader`: `.bam()`, `.bamRegion()`, `.cram()`, `.fastq()`, `.vcf()`, `.fasta()`, `.bed()` |
| `DataFrameOps.scala` | `implicit class` on `DataFrame`: `.filterRegion()`, `.filterChromosome()`, `.filterMapped`, `.filterMappingQuality()`, `.withoutAttributes`, `.filterVariantRegion()`, `.filterBedRegion()` |
| `LiteBfxSpark.scala` | Explicit object for non-implicit users: `LiteBfxSpark.read()`, `.readRegion()`, `.readCram()`, `.readFastq()`, `.readVcf()`, `.readFasta()`, `.readBed()`, `.region()` |

---

## Filesystem Compatibility

All reads go through `HadoopSeekableStream` wrapping `FSDataInputStream` from `FileSystem.get(URI, Configuration)`. This covers every Hadoop-compatible storage backend:

| Environment | Path scheme | Notes |
|---|---|---|
| Spark local | `file:///path/to/file.bam` | `LocalFileSystem` |
| Spark on HDFS | `hdfs://namenode/path/file.bam` | `DistributedFileSystem` |
| Spark on S3 | `s3a://bucket/prefix/file.bam` | `S3AFileSystem` (hadoop-aws, standard in Spark distros) |
| Databricks DBFS | `dbfs:/path/file.bam` | Pre-configured `DBFSFileSystem` on all DBR clusters |
| Databricks S3/ADLS | `s3a://` or `abfss://` | Credentials auto-configured via cluster IAM/service principal |
| Unity Catalog Volumes | `/Volumes/catalog/schema/vol/file.bam` | FUSE mount → `LocalFileSystem` (DBR 13+) |

**Critical — Hadoop Configuration propagation:** S3A/ADLS credentials live in the Spark `hadoopConfiguration`. This must be captured on the driver as `SerializableWritable<Configuration>` and stored in each `BamInputPartition` so executors can open the correct filesystem with proper credentials.

---

## Predicate Pushdown

`BamScanBuilder` implements `SupportsPushDownFilters`. BAM's BAI index enables genuine partition-level pushdown for genomic region queries.

### Pushable (resolved at partition-planning time via BAI)

| Filter | BAI call |
|---|---|
| `referenceName = 'chr1'` | Use only that reference's BAI bins |
| `start >= X AND start <= Y` + `referenceName` | `DiskBasedBAMFileIndex.getSpansForInterval(refIdx, X, Y)` |

### Non-pushable (returned as remaining; Spark evaluates row-by-row)

- `mappingQuality >= 30`
- `flags` conditions
- `start` range without `referenceName`

Spark also re-applies pushed filters as a safety pass (handles reads that span region boundaries), so all pushed filters are returned from `pushFilters()`.

**Column pruning:** Implement `SupportsPushDownRequiredColumns`. Full `SAMRecord` must be read from BAM (records can't be partially read), but `attributes` map construction can be skipped if not in the required schema.

---

## DataFrame Schema

### BAM / SAM / CRAM schema

```
readName          StringType
flags             IntegerType
referenceName     StringType
start             IntegerType   (1-based, inclusive)
mappingQuality    IntegerType
cigar             StringType
mateReferenceName StringType
mateStart         IntegerType   (1-based)
insertSize        IntegerType
sequence          StringType
baseQualities     StringType    (ASCII Phred+33)
attributes        MapType(StringType, StringType)   (SAM optional tags: NM, MD, RG, ...)
```

### FASTQ schema

```
readName          StringType    (without leading '@')
sequence          StringType
baseQualities     StringType    (ASCII Phred+33)
description       StringType    (second header line; null when absent)
```

### VCF / BCF schema

```
chrom             StringType
pos               IntegerType   (1-based)
id                StringType    (null when '.')
ref               StringType
alt               StringType    (comma-joined multi-allelic; null when '.')
qual              DoubleType    (null when '.')
filter            StringType    (semicolon-joined filter names, or "PASS"; null when '.')
info              MapType(StringType, StringType)   (INFO key=value pairs; flags stored as key="true")
format            StringType    (colon-joined FORMAT keys; null when no samples)
genotypes         MapType(StringType, StringType)   (sampleName → colon-joined FORMAT values)
```

### FASTA schema

```
name              StringType    (sequence name, without '>')
sequence          StringType    (full nucleotide string for the contig)
length            LongType      (sequence length in bases)
```

### BED schema

```
chrom             StringType
chromStart        LongType      (0-based inclusive)
chromEnd          LongType      (0-based exclusive)
name              StringType    (null when absent — BED3 files)
score             IntegerType   (null when absent — BED4 files)
strand            StringType    ("+", "-", or "."; null when absent — BED5 files)
thickStart        LongType      (null when absent — BED6 files)
thickEnd          LongType      (null when absent)
itemRgb           StringType    ("R,G,B" string; null when absent)
blockCount        IntegerType   (null when absent — BED9 files)
blockSizes        StringType    (comma-joined; null when absent)
blockStarts       StringType    (comma-joined; null when absent)
```

Fields beyond BED3 are always nullable; missing fields in a BED3/BED4/etc. file produce nulls.

---

## CRAM Support

CRAM is a reference-based compressed alignment format. `SamReaderFactory` (HTSJDK) auto-detects CRAM from file magic bytes and uses the same `SAMRecord` model as BAM. **The schema is identical to BAM/SAM.**

| Concern | Decision |
|---|---|
| Reader | `SamReaderFactory` (same as BAM) — auto-detects CRAM magic bytes |
| Index | `.cram.crai` — `CRAIIndex` in HTSJDK; virtual offset ranges analogous to BAI chunks |
| Reference | `referenceFile` option (path to `.fa`/`.fasta` + `.fai`) passed to `SamReaderFactory.referenceSource()` |
| DataSource short name | `"cram"` via `CramDataSource`; reuses all `Bam*` classes with `isCram=true` flag |
| Splitting | CRAI entries → virtual byte ranges in `BamInputPartition` |
| Fallback | Single partition when no `.cram.crai` found |

**Reference resolution order:**
1. `referenceFile` option (explicit path to FASTA — recommended)
2. MD5-based `ReferenceSource` via HTSJDK's ENA/NCBI lookup (network-dependent; not recommended in cluster contexts)
3. `referenceMode=none` — decodes without a reference; sequence bases may be unavailable for some records

`CramDataSource` registers `shortName() = "cram"` and constructs a `BamTable` with `isCram=true`. `BamScan.planInputPartitions()` branches on this flag to use `CRAIIndex` instead of `BAMIndex`.

---

## FASTQ Support

FASTQ files store raw (unaligned) sequencing reads. There is no standard seekable index for FASTQ.

### Partitioning strategy

| File type | Strategy |
|---|---|
| Uncompressed `.fastq` / `.fq` | Byte-range splitting: divide file into N equal ranges; each `FastqPartitionReader` seeks to the next `@` record boundary on open |
| Gzipped `.fastq.gz` / `.fq.gz` | Single partition (gzip is not seekable) |

`FastqScan.planInputPartitions()` detects compression from the file extension and falls back to single partition for gzipped files regardless of `numPartitions`.

### Reader

`htsjdk.samtools.fastq.FastqReader` wraps an `InputStream` obtained from a Hadoop `FSDataInputStream`. For byte-range partitions, the reader is opened at the split start, then advances to the next `@` line before beginning iteration.

### DataSource short name: `"fastq"`

---

## VCF Support

VCF (Variant Call Format) stores variant calls. BCF (binary VCF) uses the same schema. Bgzipped VCF (`.vcf.gz`) with a tabix index (`.tbi`) enables genomic range queries and multi-partition reads.

### Partitioning and predicate pushdown

| Index | Enables |
|---|---|
| Tabix `.tbi` (for `.vcf.gz`) | Region queries: `chrom` + `pos` range → virtual offset spans via `htsjdk.tribble.index.tabix.TabixIndex` |
| CSI `.csi` (for `.bcf` or `.vcf.gz`) | Same `TabixIndex` API |
| None (uncompressed `.vcf`) | Single partition, full scan |

Pushed predicates: `chrom = 'X'` and `pos >= A AND pos <= B`.

### Index resolution order (per VCF file)

1. `indexPath` option
2. Co-located `<vcfPath>.tbi`
3. Co-located `<vcfPath>.csi`
4. No index → single partition

### Reader

`htsjdk.variant.vcf.VCFFileReader` opened over a `SeekableStream`. For region queries with a tabix index, `VCFFileReader.query(chrom, start, end)` is used. Multi-sample genotypes are serialized as a `MapType(String, String)` keyed by sample name.

### DataSource short name: `"vcf"`

---

## FASTA Support

FASTA files store reference sequences. The `.fai` index (samtools FASTA index) lists every contig with its byte offset, enabling one Spark partition per contig for parallel loading.

### Partitioning strategy

| Condition | Strategy |
|---|---|
| `.fai` present | One partition per contig; `FastaPartitionReader` calls `IndexedFastaSequenceFile.getSequence(name)` |
| No `.fai` | Single partition; sequential `ReferenceSequenceFile` iteration |

### Index resolution order

1. `indexPath` option
2. Co-located `<fastaPath>.fai`
3. No index → single partition

### DataSource short name: `"fasta"`

`.fa` extension files are read with `format("fasta")`. No separate `"fa"` short name is registered (the SPI mechanism binds one short name per class; users can always use `format("fasta")` regardless of file extension).

---

## BED Support

BED (Browser Extensible Data) files store genomic intervals. HTSJDK's Tribble framework (`BEDCodec` + `AbstractFeatureReader`) handles BED parsing and reuses the same tabix infrastructure as VCF.

### Partitioning and predicate pushdown

| Index | Enables |
|---|---|
| Tabix `.tbi` (bgzipped `.bed.gz`) | Region queries: `chrom` + `chromStart`/`chromEnd` range via `TabixIndex` |
| CSI `.csi` | Same API |
| None (uncompressed `.bed`) | Single partition |

Pushed predicates: `chrom = 'X'` and `chromStart >= A AND chromEnd <= B` (interval overlap semantics).

### Index resolution order

Same as VCF: `indexPath` option → co-located `.tbi` → co-located `.csi` → single partition.

### Column count detection

BED files have no header declaring their column count. `BedPartitionReader` reads the first record to detect whether the file is BED3/4/5/6/9/12 and sets nullable fields accordingly.

### DataSource short name: `"bed"`

---

## Configuration Options

### Options common to all formats

| Option | Default | Description |
|---|---|---|
| `path` | (required) | File, directory, or glob (e.g. `s3a://bucket/*.bam`) |
| `indexPath` | _(none)_ | Explicit index path; single-file reads only |
| `indexDir` | _(none)_ | Directory of index files; `<indexDir>/<filename>.<ext>` per data file |
| `numPartitions` | `200` | Max partitions per file when index-based splitting is available |
| `useIndex` | `true` | Set `false` to force single-partition (skips index lookup) |

### BAM / CRAM additional options

| Option | Default | Description |
|---|---|---|
| `referenceFile` | _(none)_ | FASTA reference path (required for CRAM; `.fai` must be co-located) |
| `referenceMode` | `file` | `file` (use `referenceFile`), `md5` (HTSJDK ENA lookup), `none` (no reference) |

### BAI resolution order (per BAM file)

1. `indexPath` option (single-file only)
2. `indexDir/<bamFilename>.bai`
3. Co-located `<bamPath>.bai`
4. No BAI found → single partition (full scan)

### CRAI resolution order (per CRAM file)

1. `indexPath` option (single-file only)
2. `indexDir/<cramFilename>.cram.crai`
3. Co-located `<cramPath>.cram.crai`
4. No CRAI found → single partition (full scan)

### VCF / BED tabix resolution order

1. `indexPath` option (single-file only)
2. Co-located `<filePath>.tbi`
3. Co-located `<filePath>.csi`
4. No index → single partition

### FASTA FAI resolution order

1. `indexPath` option (single-file only)
2. Co-located `<fastaPath>.fai`
3. No FAI → single partition

### SAM file handling

- Auto-detected from file content — same reader code, no branching
- Always single partition (no BGZF, no index)
- BAI resolution skipped
- Mixed `.sam` / `.bam` / `.cram` directories are handled per-file

### Multi-file support (directories / globs)

When `path` is a directory or glob:
- `FileSystem.globStatus()` / `listStatus()` enumerates matching files by extension
- Each file gets its own index resolution and partition set
- Pushed predicates applied independently to each index
- Result is the union of all partitions across all files

---

## Key Implementation Notes

### `HadoopSeekableStream.java`
Implements `htsjdk.samtools.seekablestream.SeekableStream` wrapping `FSDataInputStream`. Delegates `seek(long)` → `in.seek(pos)`, `position()` → `in.getPos()`. This is the only class that needs Hadoop-filesystem-specific logic.

### `BamInputPartition.java`
```java
class BamInputPartition implements InputPartition {
    String path;
    long startVirtualOffset;   // 0 for start-of-file
    long endVirtualOffset;     // Long.MAX_VALUE for end-of-file
    SerializableWritable<Configuration> hadoopConf;
}
```

### `BamScan.planInputPartitions()`
```java
// With pushed region filter:
BAMFileSpan span = index.getSpansForInterval(refIndex, regionStart, regionEnd);
// Without filter: all chunks from all references
// No BAI: single partition [0, MAX_VALUE]
```

### `BamPartitionReader.get()` — SAMRecord → InternalRow
```java
UTF8String.fromString(record.getReadName()),
record.getFlags(),
UTF8String.fromString(record.getContig()),
record.getAlignmentStart(),
record.getMappingQuality(),
UTF8String.fromString(record.getCigarString()),
UTF8String.fromString(record.getMateContig()),
record.getMateAlignmentStart(),
record.getInferredInsertSize(),
UTF8String.fromString(record.getReadString()),
UTF8String.fromString(record.getBaseQualityString()),
// attributes map (skipped if not in required columns)
```

---

## Build

```xml
<!-- root pom.xml — parent -->
<modules><module>core</module><module>scala</module></modules>
<properties>
  <spark.version>4.0.2</spark.version>
  <scala.version>2.13.16</scala.version>
  <scala.binary.version>2.13</scala.binary.version>
  <java.version>17</java.version>
</properties>

<!-- core/pom.xml -->
<!-- spark-sql_2.13 + hadoop-client = provided -->
<!-- htsjdk = compile (shaded); relocate htsjdk.** and com.google.common.** -->
<!-- maven-compiler-plugin: source/target = 17 -->

<!-- scala/pom.xml -->
<!-- lite-bfx-spark (core) = compile (merged into fat JAR by shade) -->
<!-- spark-sql_2.13 + scala-library = provided -->
<!-- scala-maven-plugin 4.9.2: compile + testCompile -->
<!-- maven-shade-plugin: merges core JAR; strips .SF/.DSA/.RSA -->
```

```bash
mvn install -DskipTests
# → core/target/lite-bfx-spark-1.0-SNAPSHOT.jar       (Java fat JAR, htsjdk shaded)
# → scala/target/lite-bfx-spark-scala_2.13-1.0-SNAPSHOT.jar  (Scala fat JAR, includes core)

mvn test                    # Java tests (core) then Scala tests (scala)
mvn -pl core package        # core only
mvn -pl scala package       # scala only (requires core installed)
```

---

## Usage

### Python / Java (core JAR)

```python
# BAM
df = spark.read.format("bam").load("dbfs:/mnt/genomics/sample.bam")

# CRAM (referenceFile required for sequence decoding)
df = spark.read.format("cram") \
    .option("referenceFile", "s3a://ref/hg38.fa") \
    .load("s3a://data/sample.cram")

# FASTQ
df = spark.read.format("fastq").load("s3a://data/reads.fastq.gz")

# VCF with tabix pushdown
df = spark.read.format("vcf").load("s3a://data/calls.vcf.gz")
df.filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000").show()

# FASTA — each contig is one row
df = spark.read.format("fasta").load("s3a://ref/hg38.fa")

# BED with tabix pushdown
df = spark.read.format("bed").load("s3a://data/peaks.bed.gz")
df.filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000").show()

# With explicit BAI
df = spark.read.format("bam") \
    .option("indexPath", "s3a://indexes/sample.bam.bai") \
    .load("s3a://data/sample.bam")

# Cohort (multi-BAM) with BAI folder + predicate pushdown
df = spark.read.format("bam") \
    .option("indexDir", "s3a://indexes/cohort/") \
    .load("s3a://data/cohort/")
df.filter("referenceName = 'chr1' AND start >= 1000 AND start <= 5000") \
  .select("readName", "start", "cigar") \
  .show()
```

### Scala (scala fat JAR — `lite-bfx-spark-scala_2.13`)

```scala
import com.litebfx.scala.implicits._
import com.litebfx.scala.GenomicRegion

// BAM — simple read
val df = spark.read.bam("dbfs:/mnt/genomics/sample.bam")

// BAM — cohort + predicate pushdown
val reads = spark.read
  .bam("s3a://data/cohort/", indexDir = Some("s3a://indexes/cohort/"))
  .filterRegion("chr1", 1000, 5000)
  .filterMappingQuality(30)

// CRAM
val df = spark.read.cram("s3a://data/sample.cram",
  referenceFile = Some("s3a://ref/hg38.fa"))
val region = GenomicRegion("chr17", 43044295, 43125370)
df.filterRegion(region).show()

// FASTQ
val fq = spark.read.fastq("s3a://data/reads_R1.fastq.gz")
fq.select("readName", "sequence").show()

// VCF — tabix pushdown
val variants = spark.read.vcf("s3a://data/calls.vcf.gz")
  .filterVariantRegion("chr1", 1000000, 2000000)

// FASTA — one row per contig
val ref = spark.read.fasta("s3a://ref/hg38.fa")
ref.filter("name = 'chr1'").select("length").show()

// BED — tabix pushdown
val peaks = spark.read.bed("s3a://data/peaks.bed.gz")
  .filterBedRegion("chr1", 0, 1000000)

// Non-implicit API
val df = LiteBfxSpark.readRegion(spark, "s3a://data/sample.bam",
  LiteBfxSpark.region("chr1", 1000, 5000))
val vcf = LiteBfxSpark.readVcf(spark, "s3a://data/calls.vcf.gz")
val fasta = LiteBfxSpark.readFasta(spark, "s3a://ref/hg38.fa")
val bed = LiteBfxSpark.readBed(spark, "s3a://data/peaks.bed.gz")
```

---

## Verification Checklist

### BAM / SAM
1. `mvn test` — local SparkSession + bundled BAM, asserts schema + count
2. `samtools view test.bam | wc -l` equals `df.count()`
3. First 10 records match `samtools view` field-by-field
4. Upload JAR + BAM to Databricks DBR cluster → read + filter + count
5. Read from `s3a://` or `abfss://` path on Databricks
6. Rename `.bai` → single-partition fallback still reads all records
7. Read a `.sam` file → correct count, no BAI errors
8. Multi-BAM directory → count equals sum of individual `samtools view | wc -l`

### CRAM
9. `samtools view test.cram | wc -l` equals `df.count()` with `format("cram")`
10. Region query with CRAI returns same records as `samtools view -X test.cram test.cram.crai chr1:1-10000`
11. `referenceMode=none` reads without a reference without throwing
12. Missing `.cram.crai` falls back to single partition

### FASTQ
13. `wc -l test.fastq / 4` equals `df.count()`
14. First record `readName`, `sequence`, `baseQualities` match raw file
15. Gzipped `.fastq.gz` produces single partition and correct count
16. Multi-partition uncompressed read: all records present, no duplicates

### VCF
17. `bcftools view -H test.vcf.gz | wc -l` equals `df.count()`
18. Region query `chrom='chr1' AND pos >= A AND pos <= B` matches `bcftools view -r chr1:A-B`
19. INFO map contains expected keys; genotype map keyed by sample name
20. Uncompressed `.vcf` (no tabix) → single partition, correct count
21. BCF file reads correctly with `format("vcf")`

### FASTA
22. Contig count equals `grep -c '^>' ref.fa`
23. Each contig's `sequence` length matches `.fai` length column
24. `name` values match `samtools faidx` output
25. No-FAI fallback: single partition, all contigs present

### BED
26. Line count of `.bed` file equals `df.count()`
27. Region filter matches `tabix peaks.bed.gz chr1:0-1000000 | wc -l`
28. BED3 file: `name`, `score`, `strand` etc. are all null
29. BED12 file: `blockCount`, `blockSizes`, `blockStarts` populated correctly
30. Uncompressed `.bed` (no tabix) → single partition, correct count
