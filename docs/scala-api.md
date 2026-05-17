# Scala API

The `lite-bfx-spark-scala_2.13` module provides a Spark-idiomatic Scala API over the Java core. It ships as a fat JAR that includes the core DataSource V2 implementation — one JAR is all you need.

---

## Setup

Add a single import to bring all extension methods into scope:

```scala
import com.litebfx.scala.implicits._
import com.litebfx.scala.GenomicRegion   // if using typed regions
```

This follows the same opt-in pattern as `spark.implicits._`. The import wires two implicit conversions:

- `DataFrameReader → DataFrameReaderOps` — adds `.bam()`, `.cram()`, `.fastq()`, etc.
- `DataFrame → DataFrameOps` — adds `.filterRegion()`, `.filterMapped`, etc.

If you prefer to avoid wildcard imports, use the explicit `LiteBfxSpark` object instead — see [below](#litebfxspark-object).

---

## DataFrameReaderOps

Extension methods on `spark.read`.

### `.bam()`

Read a BAM or SAM file (auto-detected from content).

```scala
def bam(
  path: String,
  indexPath: Option[String] = None,
  indexDir: Option[String] = None,
  numPartitions: Int = 200,
  useIndex: Boolean = true
): DataFrame
```

```scala
// Minimal
val df = spark.read.bam("s3a://bucket/sample.bam")

// With options
val df = spark.read.bam(
  "s3a://bucket/sample.bam",
  indexPath = Some("s3a://idx/sample.bam.bai"),
  numPartitions = 50
)

// Cohort directory
val df = spark.read.bam(
  "s3a://bucket/cohort/",
  indexDir = Some("s3a://idx/cohort/")
)

// Disable index lookup
val df = spark.read.bam("s3a://bucket/sample.bam", useIndex = false)
```

### `.bamRegion()`

Read a BAM file filtered to a specific genomic region. Equivalent to `.bam(...).filterRegion(region)`.

```scala
def bamRegion(
  path: String,
  region: GenomicRegion,
  indexPath: Option[String] = None,
  indexDir: Option[String] = None,
  numPartitions: Int = 200
): DataFrame
```

```scala
val brca1 = GenomicRegion("chr17", 43044295, 43125370)
val df = spark.read.bamRegion("s3a://bucket/sample.bam", brca1)
```

### `.cram()`

Read a CRAM file.

```scala
def cram(
  path: String,
  referenceFile: Option[String] = None,
  referenceMode: String = "file",
  indexPath: Option[String] = None,
  indexDir: Option[String] = None,
  numPartitions: Int = 200
): DataFrame
```

```scala
// With reference file
val df = spark.read.cram(
  "s3a://bucket/sample.cram",
  referenceFile = Some("s3a://ref/hg38.fa")
)

// Without reference (embedded bases only)
val df = spark.read.cram(
  "s3a://bucket/sample.cram",
  referenceMode = "none"
)
```

### `.fastq()`

Read a FASTQ file. Gzipped files always use a single partition.

```scala
def fastq(path: String, numPartitions: Int = 200): DataFrame
```

```scala
val r1 = spark.read.fastq("s3a://bucket/reads_R1.fastq.gz")
val r2 = spark.read.fastq("s3a://bucket/reads_R2.fastq.gz")
```

### `.vcf()`

Read a VCF or BCF file. Tabix or CSI indexes enable region-based partition pruning.

```scala
def vcf(
  path: String,
  indexPath: Option[String] = None,
  indexDir: Option[String] = None,
  numPartitions: Int = 200,
  useIndex: Boolean = true
): DataFrame
```

```scala
val variants = spark.read.vcf("s3a://bucket/calls.vcf.gz")
val bcf = spark.read.vcf("s3a://bucket/calls.bcf")
```

### `.fasta()`

Read a FASTA file. With a co-located `.fai` index, each contig becomes one partition.

```scala
def fasta(
  path: String,
  indexPath: Option[String] = None,
  numPartitions: Int = 200
): DataFrame
```

```scala
val ref = spark.read.fasta("s3a://ref/hg38.fa")
ref.filter("name = 'chr1'").select("length").show()
```

### `.bed()`

Read a BED file. Tabix or CSI indexes enable region-based partition pruning.

```scala
def bed(
  path: String,
  indexPath: Option[String] = None,
  indexDir: Option[String] = None,
  numPartitions: Int = 200,
  useIndex: Boolean = true
): DataFrame
```

```scala
val peaks = spark.read.bed("s3a://bucket/peaks.bed.gz")
```

---

## DataFrameOps

Extension methods on `DataFrame`. All methods return a new `DataFrame` — they do not modify the receiver.

### BAM / CRAM filters

#### `.filterRegion()`

Filter to reads whose `start` falls within the given region (1-based, inclusive). The `referenceName` equality and `start` range are recognized by the BAM scan builder and trigger BAI/CRAI predicate pushdown.

```scala
def filterRegion(region: GenomicRegion): DataFrame
def filterRegion(chromosome: String, start: Int, end: Int): DataFrame
```

```scala
df.filterRegion("chr1", 1000000, 2000000)
df.filterRegion(GenomicRegion("chr17", 43044295, 43125370))
df.filterRegion(GenomicRegion.wholeChromosome("chrX"))
```

#### `.filterChromosome()`

Filter to reads aligned to a specific chromosome. Equivalent to `df.filter(col("referenceName") === chromosome)`.

```scala
def filterChromosome(chromosome: String): DataFrame
```

```scala
df.filterChromosome("chr1")
```

#### `.filterMapped`

Filter to reads that are mapped (SAM FLAG 0x4 unset). Removes unmapped reads.

```scala
def filterMapped: DataFrame
```

```scala
df.filterMapped.count()
```

#### `.filterMappingQuality()`

Filter to reads with `mappingQuality >= minMQ`. Removes multi-mappers and low-confidence alignments.

```scala
def filterMappingQuality(minMQ: Int): DataFrame
```

```scala
df.filterMappingQuality(30)
```

#### `.withoutAttributes`

Drop the `attributes` column. Applicable to BAM and CRAM DataFrames (and VCF, which has an `info` map — note this drops `attributes`, not `info`). Useful when attributes are not needed and you want to reduce memory overhead.

```scala
def withoutAttributes: DataFrame
```

```scala
df.withoutAttributes.show()
```

### VCF and BED region filters

Use plain `.filter()` — predicate pushdown to tabix is triggered automatically for `chrom`/`pos` (VCF) and `chrom`/`chromStart`/`chromEnd` (BED) predicates.

```scala
// VCF — tabix pushdown fires automatically
val variants = spark.read.vcf("s3a://bucket/calls.vcf.gz")
  .filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000")

// BED — tabix pushdown fires automatically
val peaks = spark.read.bed("s3a://bucket/peaks.bed.gz")
  .filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000")
```

---

## GenomicRegion

`GenomicRegion` is a typed value class for genomic intervals. Coordinates are **1-based, inclusive** — matching BAM `POS`, VCF `POS`, and samtools conventions.

```scala
final case class GenomicRegion(chromosome: String, start: Int, end: Int)
```

Invariants enforced at construction time:
- `start > 0`
- `end >= start`

### Constructors

```scala
// Standard interval
val region = GenomicRegion("chr17", 43044295, 43125370)

// Single-locus shorthand (start == end)
val snp = GenomicRegion("chr1", 1000000)

// Whole chromosome [1, Int.MaxValue]
val allOfChr1 = GenomicRegion.wholeChromosome("chr1")
```

### Methods

```scala
// Test overlap (same chromosome, at least one base in common)
region.overlaps(other: GenomicRegion): Boolean
```

### Convenience factory on LiteBfxSpark

```scala
val region = LiteBfxSpark.region("chr1", 1000, 5000)
// equivalent to GenomicRegion("chr1", 1000, 5000)
```

---

## LiteBfxSpark object

The explicit (non-implicit) entry point. Use this if you want to avoid wildcard imports, or when calling the Scala API from Java.

```scala
import com.litebfx.scala.LiteBfxSpark
import com.litebfx.scala.GenomicRegion
```

### Methods

```scala
// BAM
LiteBfxSpark.read(spark, path, indexPath, indexDir, numPartitions, useIndex)
LiteBfxSpark.readRegion(spark, path, region, indexPath, indexDir, numPartitions)

// CRAM
LiteBfxSpark.readCram(spark, path, referenceFile, referenceMode, indexPath, indexDir, numPartitions)

// FASTQ
LiteBfxSpark.readFastq(spark, path, numPartitions)

// VCF / BCF
LiteBfxSpark.readVcf(spark, path, indexPath, indexDir, numPartitions, useIndex)

// FASTA
LiteBfxSpark.readFasta(spark, path, indexPath, numPartitions)

// BED
LiteBfxSpark.readBed(spark, path, indexPath, indexDir, numPartitions, useIndex)

// GenomicRegion factory
LiteBfxSpark.region(chromosome, start, end)
```

All parameters match the defaults from the implicit API. Example:

```scala
val df = LiteBfxSpark.readRegion(
  spark,
  "s3a://bucket/sample.bam",
  LiteBfxSpark.region("chr1", 1000000, 2000000)
)
```

---

## Chaining filters

The extension methods return `DataFrame` so they can be chained freely with each other and with standard Spark operations:

```scala
import com.litebfx.scala.implicits._

val highQualReads = spark.read
  .bam("s3a://bucket/cohort/", indexDir = Some("s3a://idx/cohort/"))
  .filterRegion("chr17", 43044295, 43125370)  // BAI pushdown + Spark post-filter
  .filterMapped                                // row-level: unmapped flag
  .filterMappingQuality(30)                   // row-level: MAPQ >= 30
  .withoutAttributes                          // drop the tag map column
  .select("readName", "start", "cigar", "sequence")
  .cache()

highQualReads.count()
highQualReads.show(20, truncate = false)
```

---

## Notebooks

The fat Scala JAR includes the DataSource V2 implementation, so `format("bam")` etc. work without attaching the core JAR separately. Attach `lite-bfx-spark-scala_2.13-1.0-SNAPSHOT.jar` to the cluster or job, then:

```scala
// Databricks notebook (Scala)
import com.litebfx.scala.implicits._
import com.litebfx.scala.GenomicRegion

val df = spark.read.bam("dbfs:/mnt/genomics/sample.bam")
df.filterRegion("chr1", 1000000, 2000000).show()
```

```python
# Databricks notebook (Python) — using the Java API directly
df = spark.read.format("bam").load("dbfs:/mnt/genomics/sample.bam")
df.filter("referenceName = 'chr1' AND start >= 1000000 AND start <= 2000000").show()
```
