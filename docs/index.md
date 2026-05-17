# lite-bfx-spark

Native Apache Spark 4.x DataSource V2 readers for genomics file formats — BAM, SAM, CRAM, FASTQ, VCF, BCF, FASTA, and BED.

---

## Rationale

Every existing Spark genomics library is either abandoned or architecturally locked into Spark 3.x. This library is built from the ground up for Spark 4.x using the DataSource V2 API, with three goals:

1. **Read everything.** BAM/SAM/CRAM, FASTQ, VCF/BCF, FASTA, BED — the formats bioinformatics pipelines actually produce.
2. **Use indexes properly.** BAI, CRAI, tabix (.tbi/.csi), and FAI indexes are used to plan partitions and issue targeted byte-range reads against object storage. A region filter on a 50 GB BAM should not read the whole file.
3. **Work where Spark works.** All I/O goes through Hadoop `FileSystem`, so `s3a://`, `abfss://`, `gs://`, `dbfs:/`, Unity Catalog Volumes, and local paths all work without special-casing.

---

## Supported formats

| Format | Extensions | Index | `format()` |
|---|---|---|---|
| BAM | `.bam` | `.bai` | `"bam"` |
| SAM | `.sam` | _(none)_ | `"bam"` |
| CRAM | `.cram` | `.cram.crai` | `"cram"` |
| FASTQ | `.fastq`, `.fq`, `.fastq.gz`, `.fq.gz` | _(none)_ | `"fastq"` |
| VCF / BCF | `.vcf`, `.vcf.gz`, `.bcf` | `.tbi`, `.csi` | `"vcf"` |
| FASTA | `.fa`, `.fasta` | `.fai` | `"fasta"` |
| BED | `.bed`, `.bed.gz` | `.tbi`, `.csi` | `"bed"` |

---

## Quick start

=== "Python"

    ```python
    # BAM — full file
    df = spark.read.format("bam").load("s3a://bucket/sample.bam")

    # BAM — region filter pushed to BAI index
    df = (spark.read.format("bam")
            .option("indexPath", "s3a://idx/sample.bam.bai")
            .load("s3a://bucket/sample.bam")
            .filter("referenceName = 'chr1' AND start >= 1000000 AND start <= 2000000"))

    # VCF — tabix pushdown
    df = (spark.read.format("vcf")
            .load("s3a://bucket/calls.vcf.gz")
            .filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000"))

    # FASTQ
    df = spark.read.format("fastq").load("s3a://bucket/reads_R1.fastq.gz")

    # FASTA — one row per contig
    df = spark.read.format("fasta").load("s3a://ref/hg38.fa")

    # BED — tabix pushdown
    df = (spark.read.format("bed")
            .load("s3a://bucket/peaks.bed.gz")
            .filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000"))
    ```

=== "Scala"

    ```scala
    import com.litebfx.scala.implicits._
    import com.litebfx.scala.GenomicRegion

    // BAM — simple read
    val df = spark.read.bam("s3a://data/sample.bam")

    // BAM — cohort + predicate pushdown + post-filter
    val reads = spark.read
      .bam("s3a://data/cohort/", indexDir = Some("s3a://idx/cohort/"))
      .filter("referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370")
      .filter("(flags & 4) = 0")
      .filter("mappingQuality >= 30")

    // VCF — tabix pushdown
    val variants = spark.read.vcf("s3a://data/calls.vcf.gz")
      .filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000")
    ```

---

## Format documentation

- [BAM / SAM / CRAM](bam.md) — schema, region filtering, CRAM references, multi-file reads, BAI/CRAI resolution, column pruning
- [VCF / BCF](vcf.md) — schema, tabix pushdown, INFO map, genotypes map, BCF auto-detection
- [FASTA](fasta.md) — schema, FAI-based per-contig partitioning, sequence access
- [FASTQ](fastq.md) — schema, gzipped vs uncompressed, byte-range splitting, paired-end reads
- [BED](bed.md) — schema, BED3–BED12, tabix pushdown, coordinate system
- [Cloud Storage](cloud-storage.md) — S3, ADLS Gen2, GCS, DBFS, Unity Catalog Volumes, credential propagation
- [Partitioning & Indexes](partitioning.md) — how VFO-based splitting works, predicate pushdown, column pruning
- [Scala API](scala-api.md) — DataFrameReaderOps, GenomicRegion, common filter patterns
