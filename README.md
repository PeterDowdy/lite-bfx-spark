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
| [Partitioning and indexes](docs/partitioning.md) | How VFO-based splitting works, predicate pushdown mechanics, column pruning, numPartitions |
| [Cloud storage](docs/cloud-storage.md) | S3, ADLS Gen2, GCS, DBFS, Unity Catalog Volumes, credential propagation, range-request verification |
| [Scala API](docs/scala-api.md) | DataFrameReaderOps, DataFrameOps, GenomicRegion, LiteBfxSpark object |

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
| `attributes` | Map(String, String) | SAM optional tags (NM, MD, RG, …) |
| `start0` | Long | 0-based position (`start - 1`); null for unmapped. Use for BED joins. |

> **Coordinate system:** `start` is 1-based (SAM spec). BED `chromStart` is 0-based. Use `start0` when joining BAM and BED DataFrames on genomic position.

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
import com.litebfx.scala.implicits._
import com.litebfx.scala.GenomicRegion

// BAM — simple read
val df = spark.read.bam("dbfs:/mnt/genomics/sample.bam")

// BAM — cohort + predicate pushdown + post-filter
val reads = spark.read
  .bam("s3a://data/cohort/", indexDir = Some("s3a://idx/cohort/"))
  .filterRegion("chr17", 43044295, 43125370)
  .filterMappingQuality(30)
  .filterMapped

// CRAM
val df = spark.read.cram("s3a://data/sample.cram",
  referenceFile = Some("s3a://ref/hg38.fa"))
df.filterRegion(GenomicRegion("chr17", 43044295, 43125370)).show()

// FASTQ — paired-end reads side by side
val r1 = spark.read.fastq("s3a://data/reads_R1.fastq.gz")
val r2 = spark.read.fastq("s3a://data/reads_R2.fastq.gz")

// VCF — tabix pushdown
val variants = spark.read.vcf("s3a://data/calls.vcf.gz")
  .filterVariantRegion("chr1", 1000000, 2000000)

// FASTA — one row per contig
val ref = spark.read.fasta("s3a://ref/hg38.fa")
ref.filter("name = 'chr1'").select("length").show()

// BED — tabix pushdown
val peaks = spark.read.bed("s3a://data/peaks.bed.gz")
  .filterBedRegion("chr1", 0L, 1000000L)

// Non-implicit API (no import needed)
val df = LiteBfxSpark.readRegion(spark, "s3a://data/sample.bam",
  LiteBfxSpark.region("chr1", 1000, 5000))
```

### Common options

| Option | Default | Applies to | Description |
|---|---|---|---|
| `indexPath` | — | BAM, CRAM, VCF, FASTA, BED | Explicit index path; single-file reads only |
| `indexDir` | — | BAM, CRAM, VCF, BED | Directory of index files; resolved as `<dir>/<filename>.<ext>` |
| `numPartitions` | `200` | All | Max partitions per file when index-based splitting is available |
| `useIndex` | `true` | BAM, CRAM, VCF, BED | Set `false` to skip index lookup and force a single partition |
| `referenceFile` | — | CRAM | FASTA reference path; `.fai` must be co-located |
| `referenceMode` | `file` | CRAM | `file`, `md5` (ENA/NCBI lookup), or `none` |

### Partitioning behavior

| Format | With index | Without index |
|---|---|---|
| BAM | One partition per reference sequence group (capped by `numPartitions`), plus one unmapped partition | Single partition |
| BAM + region filter | Single partition (BAI-guided region query) | Single partition |
| CRAM | One partition per CRAI entry | Single partition |
| VCF / BED (bgzipped + tabix) | One or more partitions per pushed region | Single partition |
| FASTA | One partition per contig (FAI) | Single partition |
| FASTQ (uncompressed) | Byte-range splits (64 MB min, readers align to next `@`) | — |
| FASTQ (gzipped) | Single partition always (gzip is not seekable) | — |

---

## Repository structure

```
lite-bfx-spark/
├── pom.xml                     Parent POM (spark402 / spark411 profiles)
├── core/                       Java module — lite-bfx-spark.jar
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/litebfx/
│       │   ├── HadoopSeekableStream.java   htsjdk SeekableStream over FSDataInputStream
│       │   ├── SerializableConfiguration.java  Hadoop Configuration for Java serialization
│       │   ├── bam/    BAM / SAM / CRAM DataSource V2
│       │   ├── fastq/  FASTQ DataSource V2
│       │   ├── vcf/    VCF / BCF DataSource V2
│       │   ├── fasta/  FASTA DataSource V2
│       │   └── bed/    BED DataSource V2
│       ├── test/java/com/litebfx/   JUnit 5 integration tests
│       ├── test-s3/java/com/litebfx/  S3 range-access tests (MinIO, -Ps3-integration)
│       └── test/resources/          Binary fixtures (range.bam, realn01.fa, FASTQ files, …)
├── scala/                      Scala wrapper module — lite-bfx-spark-scala_2.13.jar
│   ├── pom.xml
│   └── src/
│       ├── main/scala/com/litebfx/scala/
│       │   ├── package.scala           implicits object (single import)
│       │   ├── GenomicRegion.scala     Typed genomic interval value class
│       │   ├── DataFrameReaderOps.scala  spark.read.bam/cram/fastq/vcf/fasta/bed(...)
│       │   ├── DataFrameOps.scala      df.filterRegion/filterChromosome/filterMapped/…
│       │   └── LiteBfxSpark.scala      Explicit (non-implicit) entry point
│       └── test/scala/com/litebfx/scala/  ScalaTest suites
├── tests/smoke/                Minimal Maven project — SparkSession + SELECT 1
├── docker/                     Per-runtime Dockerfiles (spark402, spark411, databricks)
├── docker-compose.yml          Test environments + cloud storage sidecars
├── Dockerfile                  Base image: Maven 3.9 + JDK 17 + samtools
├── scripts/
│   ├── generate-directory.sh   Regenerates DIRECTORY.md
│   └── run-tests.sh            Runs all Docker test environments sequentially
├── PLAN.md                     Original design document
├── IMPLEMENTATION.md           Checklist tracking implementation status
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

Both JARs are self-contained — htsjdk is shaded under `com.litebfx.shaded.htsjdk` and Guava under `com.litebfx.shaded.guava` to avoid classpath conflicts on Databricks. Spark and Hadoop are excluded (`provided` scope) and must be present in the cluster runtime.

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

### S3 range-access tests (MinIO)

These tests verify that BAI/FAI/CRAI-guided reads issue HTTP `Range` requests against object storage rather than downloading the whole file.

```bash
docker compose run --rm spark-test-s3
```

The test containers upload their own fixtures to MinIO — no manual setup required. To inspect the `Range` headers MinIO received:

```bash
docker compose logs minio | grep '"Range"'
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
| BAM splitting | BAI → virtual file offsets | No extra dependencies; htsjdk `BAMFileSpan` chunks map directly to `InputPartition` ranges |
| CRAM splitting | CRAI → `CRAIEntry` virtual offsets | Analogous to BAI; same `BamInputPartition` class with `isCram=true` |
| VCF/BED splitting | Tabix (`.tbi` / `.csi`) | Same `TabixIndex` API for both formats |
| FASTA splitting | FAI | One contig per partition; `IndexedFastaSequenceFile.getSequence()` for random access |
| FASTQ splitting | Byte-range + `@` boundary scan | No standard seekable index; readers align to the next record header |
| Filesystem | Hadoop `FileSystem` | One abstraction covers local, HDFS, S3, ADLS, GCS, DBFS, Unity Catalog Volumes |
| Shading | htsjdk → `com.litebfx.shaded.htsjdk`, Guava → `com.litebfx.shaded.guava` | Eliminates version conflicts with Databricks Runtime's bundled libraries |
| Scala API | Separate `scala/` module | Clean separation of Scala binary-version concerns from the Java core |
