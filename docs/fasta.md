# FASTA

FASTA files store reference sequences — typically a whole genome or a set of chromosomes. Each sequence (contig) begins with a `>name` header line followed by lines of nucleotide bases.

Supported extensions: `.fa`, `.fasta`. The Spark format name is always `"fasta"` regardless of extension.

---

## Schema

| Field | Type | Notes |
|---|---|---|
| `name` | String | Sequence name, without the leading `>` |
| `sequence` | String | Full nucleotide string for the contig |
| `length` | Long | Sequence length in bases |

Each row represents one complete contig. For a whole-genome reference, expect one row per chromosome.

---

## Reading FASTA files

### With a FAI index (recommended)

When a `.fai` index is present, the scan creates **one Spark partition per contig**. Each executor fetches only the bases for its assigned contig using `IndexedFastaSequenceFile.getSequence()`, which issues a targeted byte-range read. This is the efficient path for large references.

```python
# Co-located .fai is found automatically
df = spark.read.format("fasta").load("s3a://ref/hg38.fa")

# Each row is one chromosome
df.select("name", "length").orderBy("name").show()

# Fetch a specific contig
df.filter("name = 'chr1'").select("sequence").first()[0][:100]  # first 100 bases
```

### Without a FAI index

Without a `.fai` index the file is read as a single partition with sequential iteration through all contigs. This is the fallback for files that have not been indexed.

```python
df = spark.read.format("fasta").load("/data/sequences.fa")
# → 1 partition, all contigs in one task
```

### Explicit index path

```python
df = spark.read.format("fasta") \
    .option("indexPath", "s3a://idx/hg38.fa.fai") \
    .load("s3a://ref/hg38.fa")
```

This is useful when the reference and its index live in different storage locations (e.g. reference on a read-only bucket, index written to a writable bucket).

---

## FAI index resolution order

1. `indexPath` option
2. Co-located `<fastaPath>.fai`
3. No index found → single partition

---

## Spark SQL

FASTA files can be queried directly from Spark SQL without building a `DataFrameReader`.

### Direct path reference

For reads that need no options, use the `format.\`path\`` backtick syntax:

```sql
SELECT name, length
FROM fasta.`s3a://ref/hg38.fa`
ORDER BY length DESC;
```

### CREATE TEMPORARY VIEW (with options)

To pass options such as `indexPath`, create a temporary view first:

```sql
CREATE TEMPORARY VIEW reference
USING fasta
OPTIONS (
  path      's3a://ref/hg38.fa',
  indexPath 's3a://idx/hg38.fa.fai'
);

SELECT name, length(sequence) AS bases
FROM reference
WHERE name = 'chr1';
```

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `indexPath` | — | Explicit `.fai` path. |
| `numPartitions` | `200` | Maximum partitions (= contigs) when FAI is available. With a FAI, there is one partition per contig up to this limit. Has no effect when no FAI is found. |

---

## Working with sequences

The `sequence` field is the complete nucleotide string for the contig, including any soft-masked (lowercase) bases if the file uses soft masking.

```python
from pyspark.sql.functions import col, length, upper

# Confirm length matches the FAI metadata
df.select("name", "length", length("sequence").alias("actual_len"))

# GC content per contig
df.select(
    "name",
    "length",
    (
        (length(regexp_replace("sequence", "[^GCgc]", "")) / col("length")) * 100
    ).alias("gc_pct")
).show()

# Strip soft masking (convert to uppercase)
df.withColumn("sequence", upper(col("sequence")))
```

---

## Generating a FAI index

If your FASTA file does not have a `.fai` index, generate one with samtools before reading:

```bash
samtools faidx hg38.fa
# → produces hg38.fa.fai
```

The FAI format is a tab-separated text file with one line per contig:

```
name    length    byte_offset    bases_per_line    bytes_per_line
chr1    248956422    52    60    61
chr2    242193529    253000000    60    61
…
```

The byte offsets in the FAI are what allow the library to issue targeted reads for each contig without reading the whole file.

---

## Notes

- **FASTA has no region-level index.** There is no equivalent of BAI for jumping to a position within a contig. The granularity is one-partition-per-contig, not one-partition-per-region.
- **Large contigs.** A single human chromosome (chr1 ≈ 249 MB uncompressed) fits comfortably in a Spark task's heap at default settings. If you are working with particularly large contigs and see heap pressure, increase `spark.executor.memory`.
- **Compressed FASTA.** `.fa.gz` / `.fasta.gz` files are not currently supported; use uncompressed FASTA with a `.fai` index.
