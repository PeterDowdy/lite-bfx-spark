# VCF / BCF

VCF (Variant Call Format) stores variant calls from genotyping or variant detection pipelines. BCF is the binary equivalent of VCF. Both formats share an identical Spark schema and are read with `format("vcf")`.

Supported extensions: `.vcf`, `.vcf.gz`, `.bcf`.

For bgzipped (`.vcf.gz`) and BCF files, format detection is automatic: `VCFFileReader` reads the file magic bytes and selects the correct decoder. Plain-text `.vcf` files are parsed directly with a line-split reader that does not require format detection.

---

## Schema

| Field | Type | Notes |
|---|---|---|
| `chrom` | String | Chromosome / contig name (`CHROM`) |
| `pos` | Integer | 1-based position (`POS`) |
| `id` | String | Variant identifier; null when `.` |
| `ref` | String | Reference allele |
| `alt` | String | Comma-joined alternate alleles; null when `.` |
| `qual` | Double | Phred-scaled quality score; null when `.` |
| `filter` | String | `"PASS"`, or semicolon-joined filter names; null when `.` (not applied) |
| `info` | Map(String, String) | INFO field key-value pairs. Flag attributes (no value) are stored as `"true"`. Multi-value attributes are comma-joined. |
| `format` | String | Colon-joined FORMAT keys (e.g. `"GT:DP:GQ"`); null when no samples |
| `genotypes` | Map(String, String) | Sample name → colon-joined FORMAT values (e.g. `"sample1" → "0/1:32:99"`); null when no samples |

---

## Reading VCF files

### Uncompressed VCF

Uncompressed `.vcf` files are split into fixed-size byte-range partitions so multiple executors can read them in parallel. Each executor seeks to its chunk boundary, scans forward to the next newline to land on a clean line start, then parses data lines directly until the end of its chunk. The `#CHROM` header line is read from offset 0 on every executor to recover sample names before seeking to the chunk position.

The split size is controlled by `vcfSplitSize` (default 128 MiB). Files smaller than one split produce a single partition.

```python
df = spark.read.format("vcf").load("/data/calls.vcf")
df.printSchema()
df.count()

# Force smaller splits for a large plain-text VCF:
df = spark.read.format("vcf") \
    .option("vcfSplitSize", str(64 * 1024 * 1024)) \
    .load("/data/large.vcf")
```

Any pushed region filter (`chrom`, `pos`) is applied by Spark as a post-filter over each partition's output — not at the partition-planning level. All partitions are read, but only matching rows are returned. For selective queries on large plain-text VCFs, bgzipped + tabix is more efficient.

### Bgzipped VCF with tabix

Bgzipped `.vcf.gz` files with a co-located `.tbi` or `.csi` tabix index support both parallel full-file reads and targeted region queries.

**Full-file reads (no filter):** When no `chrom` filter is pushed, the scan reads the chromosome list from the tabix index on the driver and creates one partition per chromosome (grouped into at most `numPartitions`, default 200). Each executor performs a targeted tabix query for its assigned chromosomes — only the relevant BGZF blocks are transferred.

**Region queries (filter pushed):** When a `chrom` filter (and optionally a `pos` range) is pushed, a single partition is created that queries only the matching BGZF blocks.

```python
df = spark.read.format("vcf").load("s3a://bucket/calls.vcf.gz")

# Full scan — one Spark partition per chromosome, parallel reads
df.count()

# Region filter — single partition, tabix-guided I/O
df.filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000").show()

# Control the maximum number of partitions for a full scan
df = spark.read.format("vcf") \
    .option("numPartitions", "50") \
    .load("s3a://bucket/calls.vcf.gz")
```

### BCF

BCF files are read with the same `format("vcf")` name. No additional options are required.

```python
df = spark.read.format("vcf").load("s3a://bucket/calls.bcf")
df.count()
```

---

## Predicate pushdown

Pushdown behavior differs by file type:

| File type | Filter expression | Effect |
|---|---|---|
| `.vcf.gz` (tabix) | `chrom = '<value>'` | Only tabix blocks for that chromosome are read — reduces I/O |
| `.vcf.gz` (tabix) | `chrom = '<value>' AND pos >= A AND pos <= B` | Only tabix blocks overlapping `[A, B]` are read — reduces I/O |
| `.vcf` (plain) | any `chrom` / `pos` filter | Spark post-filter only — all byte-range partitions are read |

All pushed filters are re-applied by Spark as a post-filter in all cases, so results are always correct. For selective queries on large files, bgzipped + tabix is more I/O efficient than plain-text.

A `pos` range without a `chrom` filter is not pushable for tabix — tabix requires a chromosome to look up blocks.

```python
# .vcf.gz: only chr17 BGZF blocks are read from the file
df.filter("chrom = 'chr17'").count()

# .vcf.gz: only blocks overlapping BRCA1 are read
df.filter("chrom = 'chr17' AND pos >= 43044295 AND pos <= 43125370")

# .vcf (plain): all partitions read, Spark post-filters to chr17 rows
df.filter("chrom = 'chr17'").count()

# Not pushed for tabix: pos range without chrom — full scan
df.filter("pos >= 1000000 AND pos <= 2000000")
```

---

## Index resolution order

Index resolution applies only to bgzipped (`.vcf.gz`) and BCF files. Plain-text `.vcf` files always use byte-range splitting regardless of whether an index is present.

For each bgzipped or BCF file:

1. `indexPath` option (single-file reads only)
2. Co-located `<filePath>.tbi`
3. Co-located `<filePath>.csi`
4. No index → single partition, full-file scan

```python
# Explicit tabix path
df = spark.read.format("vcf") \
    .option("indexPath", "s3a://idx/calls.vcf.gz.tbi") \
    .load("s3a://bucket/calls.vcf.gz")

# Disable index lookup
df = spark.read.format("vcf") \
    .option("useIndex", "false") \
    .load("s3a://bucket/calls.vcf.gz")
```

---

## Working with the INFO map

The `info` column is a `MapType(StringType, StringType)`. All INFO values are stored as strings regardless of the declared VCF type.

```python
from pyspark.sql.functions import col

# Select a specific INFO field
df.select("chrom", "pos", "ref", "alt", col("info")["AF"].alias("AF"))

# Filter on an INFO field value
df.filter(col("info")["DP"].cast("int") >= 20)

# Flag INFO fields are stored as the string "true"
df.filter(col("info")["SOMATIC"] == "true")

# Multi-value INFO fields are comma-joined strings
# INFO=AF=0.1,0.2 → info["AF"] = "0.1,0.2"
```

---

## Working with genotypes

The `genotypes` column is a `MapType(StringType, StringType)` keyed by sample name. Values are the FORMAT field values joined by `:` in the same order as the `format` column.

The `format` column contains the colon-joined FORMAT keys (e.g. `"GT:DP:GQ"`).

```python
from pyspark.sql.functions import col, split

# Get the genotype string for a specific sample
df.select("chrom", "pos", col("genotypes")["sample1"].alias("gt_sample1"))

# Parse out the GT field (always first in FORMAT)
df.select(
    "chrom", "pos",
    split(col("genotypes")["sample1"], ":")[0].alias("GT")
)

# Filter to heterozygous calls (GT = "0/1" or "0|1")
df.filter(
    split(col("genotypes")["sample1"], ":")[0].isin("0/1", "0|1")
)
```

For multi-sample VCF files, all samples appear as keys in the genotypes map within each row.

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `vcfSplitSize` | `134217728` (128 MiB) | Byte-range split size for plain-text `.vcf` files. Smaller values increase parallelism; ignored for `.vcf.gz` and `.bcf`. |
| `numPartitions` | `200` | Maximum number of partitions for bgzipped VCF full-file reads (per-chromosome partitioning). Has no effect on plain-text `.vcf` files or region queries. |
| `indexPath` | — | Explicit tabix index path (`.tbi` or `.csi`). Applies to bgzipped files only. Single-file reads only. |
| `useIndex` | `true` | Set `false` to skip index resolution for bgzipped/BCF files and read the whole file in one partition. Has no effect on plain-text `.vcf` files. |
