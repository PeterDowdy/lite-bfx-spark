# VCF / BCF

VCF (Variant Call Format) stores variant calls from genotyping or variant detection pipelines. BCF is the binary equivalent of VCF. Both formats share an identical Spark schema and are read with `format("vcf")`.

Supported extensions: `.vcf`, `.vcf.gz`, `.bcf`.

Format detection is automatic: `VCFFileReader` reads the file magic bytes and selects the correct decoder. No format hint or special option is needed to distinguish VCF from BCF.

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

Uncompressed `.vcf` files have no seekable index. They always produce a single partition.

```python
df = spark.read.format("vcf").load("/data/calls.vcf")
df.printSchema()
df.count()
```

### Bgzipped VCF with tabix

Bgzipped `.vcf.gz` files with a co-located `.tbi` or `.csi` tabix index support region queries. The tabix index is used to produce partition plans and push region filters down to BGZF block level.

```python
df = spark.read.format("vcf").load("s3a://bucket/calls.vcf.gz")

# Region filter — chrom equality and pos range are pushed to the tabix index
df.filter("chrom = 'chr1' AND pos >= 1000000 AND pos <= 2000000").show()
```

### BCF

BCF files are read with the same `format("vcf")` name. No additional options are required.

```python
df = spark.read.format("vcf").load("s3a://bucket/calls.bcf")
df.count()
```

---

## Predicate pushdown

The scan builder recognizes two filter patterns and uses them to plan tabix-guided partitions:

| Filter expression | Pushdown effect |
|---|---|
| `chrom = '<value>'` | Only tabix blocks for that chromosome are read |
| `chrom = '<value>' AND pos >= A AND pos <= B` | Only tabix blocks overlapping `[A, B]` are read |

All pushed filters are also re-applied by Spark as a post-filter to handle records that span block boundaries. This means the result is always correct, and pushing filters is always safe.

A `pos` range without a `chrom` filter is not pushable — tabix requires a chromosome to look up blocks.

```python
# Pushed: only chr17 BGZF blocks are read from the file
df.filter("chrom = 'chr17'").count()

# Pushed: only blocks overlapping BRCA1 are read
df.filter("chrom = 'chr17' AND pos >= 43044295 AND pos <= 43125370")

# Not pushed: pos range without chrom — full scan, Spark post-filters
df.filter("pos >= 1000000 AND pos <= 2000000")
```

---

## Index resolution order

For each VCF/BCF file:

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
| `indexPath` | — | Explicit tabix index path (`.tbi` or `.csi`). Single-file reads only. |
| `indexDir` | — | Directory of tabix index files. Resolved as `<indexDir>/<filename>.tbi`. |
| `numPartitions` | `200` | Maximum partitions per file when tabix splitting is active. Currently a single partition is always planned per pushed region; this option is reserved for future multi-block splitting. |
| `useIndex` | `true` | Set `false` to skip index resolution and read the whole file in one partition. |
