# lite-bfx-spark

Native Apache Spark 4.x DataSource V2 readers for genomics file formats — BAM, SAM, CRAM, FASTQ, VCF, BCF, FASTA, and BED.

## Rationale

Every existing Spark genomics library I could find was either abandoned or architecturally locked into Spark 3.x. This library is built from the ground up for Spark 4.x using the DataSource V2 API, with three goals:

1. **Read everything.** BAM/SAM/CRAM, FASTQ, VCF/BCF, FASTA, BED — the formats bioinformatics pipelines actually produce.
2. **Use indexes properly.** BAI, CRAI, tabix (.tbi/.csi), and FAI indexes are used to plan partitions and issue targeted byte-range reads against object storage. A region filter on a 50 GB BAM should not read the whole file.
3. **Work where Spark works.** All I/O goes through Hadoop `FileSystem`, so `s3a://`, `abfss://`, `gs://`, `dbfs:/`, Unity Catalog Volumes, and local paths all work without special-casing.

The library does **not** use Disq or hadoop-bam. Those add transitive dependencies that conflict on Databricks. htsjdk alone is sufficient when you implement index-based splitting yourself, and it is shaded into the JAR to eliminate version conflicts entirely.

---

## Documentation

| Document | Contents |
|---|---|
| [BAM / SAM / CRAM](docs/bam.md) | Schema, region filtering, CRAM reference options, multi-file reads, partition planning, BAI/CRAI resolution, column pruning |
| [FASTQ](docs/fastq.md) | Schema, gzipped vs uncompressed, byte-range splitting, paired-end reads |
| [VCF / BCF](docs/vcf.md) | Schema, tabix pushdown, INFO map, genotypes map, BCF auto-detection |
| [FASTA](docs/fasta.md) | Schema, FAI-based per-contig partitioning, sequence access |
| [BED](docs/bed.md) | Schema, BED3–BED12, tabix pushdown, coordinate system |
| [Partitioning and indexes](docs/partitioning.md) | Index-guided (VFO) splitting, index-free splitting of unindexed BAM/CRAM/SAM, predicate pushdown mechanics, column pruning, numPartitions |
| [Cloud storage](docs/cloud-storage.md) | S3, ADLS Gen2, GCS, DBFS, Unity Catalog Volumes, credential propagation, range-request verification |
| [Scala API](docs/scala-api.md) | DataFrameReaderOps, GenomicRegion, LiteBfxSpark object, common filter patterns |
| [Architecture](ARCHITECTURE.md) | DataSource V2 layer diagram, module layout, index strategy, design decisions |

For testing infrastructure, see [TESTING.md](TESTING.md).

---

## Supported formats

| Format | Extensions | Index | Spark `format()` |
|---|---|---|---|
| BAM | `.bam` | `.bai` | `"bam"` |
| SAM | `.sam` | _(none)_ | `"bam"` (auto-detected) |
| CRAM | `.cram` | `.cram.crai` | `"cram"` |
| FASTQ | `.fastq`, `.fq`, `.fastq.gz`, `.fq.gz` | _(none)_ | `"fastq"` |
| VCF / BCF | `.vcf`, `.vcf.gz`, `.bcf` | `.tbi`, `.csi` | `"vcf"` |
| FASTA | `.fa`, `.fasta` | `.fai` | `"fasta"` |
| BED | `.bed`, `.bed.gz` | `.tbi`, `.csi` | `"bed"` |

---

## Schemas

### BAM / SAM / CRAM

| Field | Type | Notes |
|---|---|---|
| `readName` | String | |
| `flags` | Integer | SAM bitfield |
| `referenceName` | String | |
| `start` | Long | 1-based inclusive (SAM spec) |
| `mappingQuality` | Integer | |
| `cigar` | String | |
| `mateReferenceName` | String | |
| `mateStart` | Long | 1-based (SAM spec) |
| `insertSize` | Integer | |
| `sequence` | String | |
| `baseQualities` | String | ASCII Phred+33 |
| `attributes` | Map(String, String) | SAM optional tags (NM, MD, RG, …); values are SAM `TYPE:VALUE` form, e.g. `NM → "i:3"`, `RG → "Z:1"` |
| `start0` | Long | 0-based position (`start - 1`); null for unmapped. Use for BED joins. |

> **Coordinate system:** `start` is 1-based (SAM spec). BED `chromStart` is 0-based. Use `start0` when joining BAM and BED DataFrames on genomic position.

> **Column naming:** Set `.option("columnNames", "sam")` to use canonical SAM-spec field names (`qname`, `flag`, `rname`, `pos`, `mapq`, `cigar`, `rnext`, `pnext`, `tlen`, `seq`, `qual`) instead of the descriptive defaults. See [docs/bam.md](docs/bam.md#column-naming).

> **File metadata:** A hidden, Databricks-compatible `_metadata` struct (`file_path`, `file_name`, `file_size`, `file_modification_time`) is available on every read — e.g. `df.select("_metadata.file_path")`. See [docs/bam.md](docs/bam.md#file-metadata-_metadata).

### FASTQ

| Field | Type | Notes |
|---|---|---|
| `readName` | String | Without leading `@` |
| `sequence` | String | |
| `baseQualities` | String | ASCII Phred+33 |
| `description` | String | Second header line; null when absent |

### VCF / BCF

| Field | Type | Notes |
|---|---|---|
| `chrom` | String | |
| `pos` | Integer | 1-based |
| `id` | String | Null when `.` |
| `ref` | String | |
| `alt` | Array(String) | Alternate alleles; null when `.` |
| `qual` | Double | Null when `.` |
| `filter` | String | `"PASS"` or semicolon-joined; null when `.` |
| `info` | Map(String, String) | INFO key=value pairs; flags stored as `"true"` |
| `format` | String | Colon-joined FORMAT keys; null when no samples |
| `genotypes` | Map(String, String) | sampleName → colon-joined FORMAT values |

### FASTA

| Field | Type | Notes |
|---|---|---|
| `name` | String | Sequence name, without `>` |
| `sequence` | String | Full nucleotide string |
| `length` | Long | Length in bases |

### BED

| Field | Type | Notes |
|---|---|---|
| `chrom` | String | |
| `chromStart` | Long | 0-based inclusive |
| `chromEnd` | Long | 0-based exclusive |
| `name` | String | Null in BED3 files |
| `score` | Integer | Null in BED3/4 files |
| `strand` | String | `"+"`, `"-"`, or `"."`; null in BED3–5 |
| `thickStart` | Long | Null in BED3–6 |
| `thickEnd` | Long | Null in BED3–6 |
| `itemRgb` | String | `"R,G,B"`; null in BED3–8 |
| `blockCount` | Integer | Null in BED3–9 |
| `blockSizes` | String | Comma-joined; null in BED3–9 |
| `blockStarts` | String | Comma-joined; null in BED3–9 |

Fields beyond BED3 are nullable; missing columns in BED3/4/5/6/9 files produce nulls.

---

## Usage

### Python / Java (core JAR)

```python
# BAM — full file
df = spark.read.format("bam").load("s3a://bucket/sample.bam")

# BAM — region filter (pushed to BAI index)
df = spark.read.format("bam") \
    .option("indexPath", "s3a://idx/sample.bam.bai") \
    .load("s3a://bucket/sample.bam") \
    .filter("referenceName = 'chr1' AND start >= 1000000 AND start <= 2000000")

# BAM — cohort directory with shared index folder
df = spark.read.format("bam") \
    .option("indexDir", "s3a://idx/cohort/") \
    .load("s3a://bucket/cohort/")

# CRAM (referenceFile required for sequence decoding)
df = spark.read.format("cram") \
    .option("referenceFile", "s3a://ref/hg38.fa") \
    .load("s3a://bucket/sample.cram")

# FASTQ
df = spark.read.format("fastq").load("s3a://bucket/reads_R1.fastq.gz")

# VCF — with tabix pushdown
df = spark.read.format("vcf").load("s3a://bucket/calls.vcf.gz") \
    .filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000")

# BCF — same format name, auto-detected from magic bytes
df = spark.read.format("vcf").load("s3a://bucket/calls.bcf")

# FASTA — one row per contig
df = spark.read.format("fasta").load("s3a://ref/hg38.fa")

# BED — with tabix pushdown
df = spark.read.format("bed").load("s3a://bucket/peaks.bed.gz") \
    .filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000")
```

### Scala (scala fat JAR)

```scala
import io.github.peterdowdy.litebfx.scala.implicits._
import io.github.peterdowdy.litebfx.scala.GenomicRegion

// BAM — simple read
val df = spark.read.bam("dbfs:/mnt/genomics/sample.bam")

// BAM — cohort + predicate pushdown + post-filter
val reads = spark.read
  .bam("s3a://data/cohort/", indexDir = Some("s3a://idx/cohort/"))
  .filter("referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370")
  .filter("(flags & 4) = 0")
  .filter("mappingQuality >= 30")

// CRAM
val df = spark.read.cram("s3a://data/sample.cram",
  referenceFile = Some("s3a://ref/hg38.fa"))
df.filter("referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370").show()

// FASTQ — paired-end reads side by side
val r1 = spark.read.fastq("s3a://data/reads_R1.fastq.gz")
val r2 = spark.read.fastq("s3a://data/reads_R2.fastq.gz")

// VCF — tabix pushdown (plain .filter triggers it automatically)
val variants = spark.read.vcf("s3a://data/calls.vcf.gz")
  .filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000")

// FASTA — one row per contig
val ref = spark.read.fasta("s3a://ref/hg38.fa")
ref.filter("name = 'chr1'").select("length").show()

// BED — tabix pushdown (plain .filter triggers it automatically)
val peaks = spark.read.bed("s3a://data/peaks.bed.gz")
  .filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000")

// Non-implicit API (no import needed)
val df = LiteBfxSpark.readRegion(spark, "s3a://data/sample.bam",
  LiteBfxSpark.region("chr1", 1000, 5000))
```

### Common options

| Option | Default | Applies to | Description |
|---|---|---|---|
| `indexPath` | — | BAM, CRAM, VCF, FASTA, BED | Explicit index path; single-file reads only |
| `indexDir` | — | BAM, CRAM, VCF, BED | Directory of index files; resolved as `<dir>/<filename>.<ext>` |
| `numPartitions` | `200` | BAM, CRAM, VCF, FASTQ | Max partitions for index-guided splits (BAM reference groups, CRAM container groups, tabix chromosome groups). Does **not** cap byte-range splits of unindexed BAM/SAM |
| `useIndex` | `true` | BAM, CRAM, VCF, BED | Set `false` to ignore any index. The file is then split by byte range (BAM/SAM) or a container-header scan (CRAM) — not read as a single partition |
| `vcfSplitSize` | `134217728` | VCF | Byte-range chunk size (bytes) for plain-text VCF splits |
| `bedSplitSize` | `134217728` | BED | Byte-range chunk size (bytes) for plain-text BED splits |
| `minSplitBytes` | `67108864` | FASTQ | Minimum partition size (bytes) for uncompressed FASTQ byte-range splits |
| `bgzfSplitSize` | `134217728` | FASTQ, BAM | Compressed-byte chunk size for BGZF splits: `.gz` FASTQ **and** unindexed BAM |
| `samSplitSize` | `134217728` | SAM | Byte-range chunk size (bytes) for plain-text SAM splits |
| `referenceFile` | — | CRAM | FASTA reference path; `.fai` must be co-located |
| `referenceMode` | `file` | CRAM | `file`, `md5` (ENA/NCBI lookup), or `none` |

### Partitioning behavior

| Format | Scenario | Partitions |
|---|---|---|
| BAM | BAI present, no region filter | One per reference group (capped by `numPartitions`), plus unmapped |
| BAM | BAI present, region filter pushed | Single (BAI-guided range query) |
| BAM | No BAI, or `useIndex=false` | BGZF block-range splits (128 MiB default; tune with `bgzfSplitSize`) — see below |
| SAM | Always (no index format exists) | Line-based byte-range splits (128 MiB default; tune with `samSplitSize`) |
| CRAM | CRAI present, no region filter | Container groups (capped by `numPartitions`) |
| CRAM | CRAI present, region filter pushed | Single (CRAI-guided range query) |
| CRAM | No CRAI, or `useIndex=false` | Container groups from a driver-side header scan (capped by `numPartitions`) — see below |
| VCF (plain-text `.vcf`) | — | Byte-range splits (128 MiB default; tune with `vcfSplitSize`) |
| VCF (bgzipped, tabix present, no region filter) | — | One per chromosome group (capped by `numPartitions`) |
| VCF (bgzipped, tabix present, region filter pushed) | — | Single (tabix-guided range query) |
| VCF (bgzipped, no tabix) | — | Single |
| BED (plain-text `.bed`) | — | Byte-range splits (128 MiB default; tune with `bedSplitSize`) |
| BED (bgzipped, tabix present, region filter pushed) | — | Single (tabix-guided range query) |
| BED (bgzipped, no tabix or no pushed filter) | — | Single |
| FASTA | FAI present | One per contig |
| FASTA | No FAI | Single |
| FASTQ (uncompressed) | — | Byte-range splits (min 64 MiB; capped by `numPartitions`) |
| FASTQ (BGZF `.gz`, file ≥ `bgzfSplitSize`) | — | Byte-range splits at BGZF block boundaries (128 MiB; capped by `numPartitions`) |
| FASTQ (regular `.gz` or small BGZF) | — | Single (gzip is not seekable) |

#### Splitting non-indexed BAM and CRAM

An unindexed BAM or CRAM is no longer read in a single partition — it is split for parallelism even without an index, and the same path is taken when you set `useIndex=false`.

**BAM without a BAI** is divided into fixed-size byte chunks of `bgzfSplitSize` (default 128 MB), producing `ceil(fileSize / bgzfSplitSize)` partitions. This count is **not** capped by `numPartitions`. Chunk boundaries are computed arithmetically (`i × bgzfSplitSize`) rather than aligned to real BGZF block offsets — deliberately, because enumerating block offsets would force the driver to scan the whole file (thousands of seeks on object storage) just to plan. Instead each executor orients itself locally: it reads at most ~65 KB (one HTTP `Range` request) from its chunk start to find the next BGZF block that begins a clean BAM record, then reads records until the next chunk boundary. A record straddling a boundary is claimed by exactly one partition, so the split is lossless.

**CRAM without a CRAI** is split by container. The driver makes one lightweight pass over the file reading only container *headers* (via htsjdk's `CramContainerHeaderIterator` — no record decoding) to collect container byte offsets, then distributes those containers into at most `numPartitions` contiguous groups. Each executor decodes only the containers in its assigned byte span. This requires a sequential header pass on the driver (cheap — headers only), but yields balanced, container-aligned partitions.

In both cases the entire file is still read across the union of partitions: an index-free split buys **read parallelism, not I/O reduction**. To prune I/O (read only the regions you ask for), supply a `.bai`/`.crai` and push a region filter. Setting `bgzfSplitSize` below the maximum BGZF block size (~65 KB) is counterproductive — many chunks scan ~65 KB only to find no clean record start and yield zero rows; the 128 MB default suits typical WGS BAMs.

---

## Repository structure

```
lite-bfx-spark/
├── pom.xml                     Parent POM (spark402 / spark411 profiles)
├── core/                       Java module — lite-bfx-spark.jar
│   ├── pom.xml
│   └── src/
│       ├── main/java/io/github/peterdowdy/litebfx/
│       │   ├── HadoopSeekableStream.java   htsjdk SeekableStream over FSDataInputStream
│       │   ├── SerializableConfiguration.java  Hadoop Configuration for Java serialization
│       │   ├── bam/    BAM / SAM / CRAM DataSource V2
│       │   ├── fastq/  FASTQ DataSource V2
│       │   ├── vcf/    VCF / BCF DataSource V2
│       │   ├── fasta/  FASTA DataSource V2
│       │   └── bed/    BED DataSource V2
│       ├── test/java/io/github/peterdowdy/litebfx/          JUnit 5 integration tests
│       ├── test-s3/java/io/github/peterdowdy/litebfx/       S3 range-access tests (MinIO, -Ps3-integration)
│       ├── test-gcs/java/io/github/peterdowdy/litebfx/      GCS range-access tests (fake-gcs-server, -Pgcs-integration)
│       ├── test-azure/java/io/github/peterdowdy/litebfx/    Azure range-access tests (Azurite, -Pazure-integration)
│       └── test/resources/                 Binary fixtures (range.bam, realn01.fa, FASTQ files, …)
├── scala/                      Scala wrapper module — lite-bfx-spark-scala_2.13.jar
│   ├── pom.xml
│   └── src/
│       ├── main/scala/io/github/peterdowdy/litebfx/scala/
│       │   ├── package.scala           implicits object (single import)
│       │   ├── GenomicRegion.scala     Typed genomic interval value class
│       │   ├── DataFrameReaderOps.scala  spark.read.bam/cram/fastq/vcf/fasta/bed(...)
│       │   └── LiteBfxSpark.scala      Explicit (non-implicit) entry point
│       └── test/scala/io/github/peterdowdy/litebfx/scala/  ScalaTest suites
├── tests/smoke/                Minimal Maven project — SparkSession + SELECT 1
├── docker/                     Per-runtime Dockerfiles (spark402, spark411, databricks)
├── docker-compose.yml          Test environments + cloud storage sidecars
├── Dockerfile                  Base image: Maven 3.9 + JDK 17 + samtools
├── scripts/
│   ├── generate-directory.sh   Regenerates DIRECTORY.md
│   └── run-tests.sh            Runs all Docker test environments sequentially
├── ARCHITECTURE.md             Architecture overview and design decisions
├── DIRECTORY.md                Auto-generated file index with descriptions
└── TESTING.md                  Full testing guide
```

Each format follows the same five-class DataSource V2 pattern:

```
<Format>DataSource      → TableProvider + DataSourceRegister (registers the short name)
<Format>Table           → Table + SupportsRead
<Format>ScanBuilder     → ScanBuilder + SupportsPushDownFilters + SupportsPushDownRequiredColumns
<Format>Scan            → Scan + Batch (plans InputPartitions from index)
<Format>InputPartition  → InputPartition + Serializable (byte range + Hadoop config)
<Format>PartitionReaderFactory → PartitionReaderFactory
<Format>PartitionReader → PartitionReader<InternalRow> (htsjdk → Spark InternalRow)
```

---

## Development setup

All builds and tests run inside Docker. No local JDK or Maven is required.

**Install Docker Desktop** (Mac: whale icon in menu bar must stop animating before proceeding):

```bash
# Verify install
docker compose version

# Build all test images (one time, ~5–10 min on first run)
docker compose build spark-test spark402 spark411 databricks
```

The `maven-cache` Docker volume persists the local Maven repository between runs so dependencies are only downloaded once.

---

## Building

```bash
# Build both modules, skip tests (fastest)
docker compose run --rm spark-test mvn install -DskipTests

# Build against Spark 4.1.1
docker compose run --rm spark411 mvn install -DskipTests -Pspark411
```

Output JARs:

| JAR | Contents | Use case |
|---|---|---|
| `core/target/lite-bfx-spark-1.0-SNAPSHOT.jar` | Java DataSource V2 + shaded htsjdk | Python, Java, PySpark |
| `scala/target/lite-bfx-spark-scala_2.13-1.0-SNAPSHOT.jar` | Scala API + core (merged) | Scala, notebooks |

Both JARs are self-contained — htsjdk is shaded under `io.github.peterdowdy.litebfx.shaded.htsjdk` and Guava under `io.github.peterdowdy.litebfx.shaded.guava` to avoid classpath conflicts on Databricks. Spark and Hadoop are excluded (`provided` scope) and must be present in the cluster runtime.

---

## Running tests

### Full test suite

```bash
# Default: Spark 4.0.2 inside the base Maven image
docker compose run --rm spark-test mvn test -Pspark402

# Against a specific Spark runtime
docker compose run --rm spark402    mvn test -Pspark402
docker compose run --rm spark411    mvn test -Pspark411
docker compose run --rm databricks  mvn test -Pspark402
```

### Single test class

```bash
docker compose run --rm spark-test mvn test -pl core -Pspark402 -Dtest=BamDataSourceTest
```

### Cloud storage range-access tests

These tests verify that BAI/FAI/CRAI/tabix-guided reads issue HTTP `Range` requests against object storage rather than downloading the whole file. Each suite uploads its own fixtures — no manual setup required.

**S3 (MinIO)**

```bash
docker compose run --rm spark-test-s3
```

To inspect the `Range` headers MinIO received:

```bash
docker compose logs minio | grep '"Range"'
```

**GCS (fake-gcs-server)**

```bash
docker compose run --rm spark-test-gcs
```

**Azure Blob Storage (Azurite)**

```bash
docker compose run --rm spark-test-azure
```

### Databricks Serverless smoke test

Requires a real Databricks workspace and a personal access token or OAuth M2M credentials:

```bash
docker compose run --rm \
  -e DATABRICKS_HOST=https://<workspace>.azuredatabricks.net \
  -e DATABRICKS_TOKEN=<token> \
  databricks-connect python3 tests/smoke_serverless.py
```

### Interactive shell

```bash
docker compose run --rm spark-shell   # drops into bash inside the spark-test image
```

---

## Deploying to a cluster

### Databricks

Upload the JAR to DBFS or a Unity Catalog Volume, then attach it to a cluster or job:

```bash
# DBFS (classic clusters)
databricks fs cp core/target/lite-bfx-spark-1.0-SNAPSHOT.jar \
  dbfs:/FileStore/jars/lite-bfx-spark.jar

# Unity Catalog Volume (recommended for DBR 13+)
databricks fs cp core/target/lite-bfx-spark-1.0-SNAPSHOT.jar \
  /Volumes/<catalog>/<schema>/<volume>/lite-bfx-spark.jar
```

For Databricks Serverless jobs, JARs must live in a Unity Catalog Volume or workspace files — not `dbfs:/`.

### Apache Spark (standalone / YARN / Kubernetes)

Pass the JAR via `--jars` or `spark.jars`:

```bash
spark-submit \
  --jars core/target/lite-bfx-spark-1.0-SNAPSHOT.jar \
  your_job.py
```

Or add it to `spark-defaults.conf`:

```
spark.jars=/path/to/lite-bfx-spark-1.0-SNAPSHOT.jar
```

### Cloud object storage configuration

The Hadoop `FileSystem` used for reading picks up credentials from Spark's `hadoopConfiguration`. Each `InputPartition` carries a serialized copy of this configuration so executor-side reads get the same credentials as the driver.

**S3 (s3a://)**

```
spark.hadoop.fs.s3a.endpoint=https://s3.amazonaws.com   # or MinIO endpoint
spark.hadoop.fs.s3a.access.key=...
spark.hadoop.fs.s3a.secret.key=...
```

On EMR and Databricks, IAM roles or instance profiles handle this automatically — no explicit credentials needed.

**Azure ADLS Gen2 (abfss://)**

```
spark.hadoop.fs.azure.account.auth.type.<account>.dfs.core.windows.net=OAuth
spark.hadoop.fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net=...
```

**GCS (gs://)**

```
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.gs.auth.service.account.enable=true
```

---

## Technology choices

| Concern | Choice | Why |
|---|---|---|
| Language | Java 17 | Required for Spark 4.0; avoids Scala binary-version matrix in the core JAR |
| Build | Maven + shade plugin | Bundles htsjdk; avoids classpath conflicts on Databricks |
| Spark API | DataSource V2 | Proper partitioning, predicate pushdown, column pruning — not available in V1 |
| Parsing | htsjdk 4.3.0 | Industry standard; single library covers BAM/SAM/CRAM/FASTQ/VCF/BCF/FASTA/BED |
| BAM splitting | BAI → virtual file offsets (indexed); BGZF byte-range chunks (unindexed) | htsjdk `BAMFileSpan` chunks map directly to `InputPartition` ranges; unindexed files split into `bgzfSplitSize` chunks with executor-side block alignment |
| CRAM splitting | CRAI → `CRAIEntry` virtual offsets (indexed); container-header scan (unindexed) | Analogous to BAI; container offsets grouped into `numPartitions` partitions; same `BamInputPartition` class with `isCram=true` |
| VCF/BED splitting | Tabix (`.tbi` / `.csi`) | Same `TabixIndex` API for both formats |
| FASTA splitting | FAI | One contig per partition; `IndexedFastaSequenceFile.getSequence()` for random access |
| FASTQ splitting | Byte-range + `@` boundary scan | No standard seekable index; readers align to the next record header |
| Filesystem | Hadoop `FileSystem` | One abstraction covers local, HDFS, S3, ADLS, GCS, DBFS, Unity Catalog Volumes |
| Shading | htsjdk → `io.github.peterdowdy.litebfx.shaded.htsjdk`, Guava → `io.github.peterdowdy.litebfx.shaded.guava` | Eliminates version conflicts with Databricks Runtime's bundled libraries |
| Scala API | Separate `scala/` module | Clean separation of Scala binary-version concerns from the Java core |
