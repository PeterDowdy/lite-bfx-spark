# FASTQ

FASTQ files store raw (unaligned) sequencing reads, each as four lines: a header, the base sequence, a separator (`+`), and the base quality scores.

Supported extensions: `.fastq`, `.fq`, `.fastq.gz`, `.fq.gz`.

---

## Schema

| Field | Type | Notes |
|---|---|---|
| `readName` | String | Read identifier, without the leading `@` |
| `sequence` | String | Base sequence |
| `baseQualities` | String | ASCII Phred+33 quality scores |
| `description` | String | Second header token (text after the first space on the `@` line); null when absent |

---

## Reading FASTQ files

```python
# Gzipped — single partition
df = spark.read.format("fastq").load("s3a://bucket/reads_R1.fastq.gz")

# Uncompressed — byte-range splits
df = spark.read.format("fastq") \
    .option("numPartitions", "32") \
    .load("s3a://bucket/reads.fastq")

df.select("readName", "sequence", "baseQualities").show(5, truncate=False)
```

---

## Partitioning

FASTQ has no seekable index. The split strategy depends on whether the file is gzip-compressed.

### Gzipped files (`.fastq.gz`, `.fq.gz`)

Gzip streams cannot be seeked into, so gzipped FASTQ files always produce **a single partition** regardless of `numPartitions`. The entire file is decompressed and read sequentially by one executor.

### Uncompressed files (`.fastq`, `.fq`)

The file is divided into byte-range splits at boundaries no smaller than 64 MB. The actual number of splits is `min(numPartitions, ceil(fileSize / 64MB))`.

When each executor opens its assigned byte range, it scans forward from the split start to find the next `@` line that begins a FASTQ record. This boundary scan ensures records are not duplicated or dropped across splits regardless of where the byte boundary falls. Reads stop when the stream position exceeds the split's end byte.

```
Split 0:  bytes [0,      64MB)  → scans to first @, reads until position > 64MB
Split 1:  bytes [64MB,  128MB)  → scans to first @, reads until position > 128MB
…
```

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `numPartitions` | `200` | Maximum partitions for uncompressed files. Ignored for gzipped files (always 1 partition). Actual partition count is `min(numPartitions, ceil(fileSize / 64MB))`. |

No `indexPath`, `indexDir`, or `useIndex` options are supported — FASTQ has no standard index.

---

## Paired-end reads

FASTQ does not interleave paired reads. R1 and R2 are stored in separate files. To work with pairs, read each file independently and join on `readName`:

```python
r1 = spark.read.format("fastq").load("s3a://bucket/reads_R1.fastq.gz")
r2 = spark.read.format("fastq").load("s3a://bucket/reads_R2.fastq.gz")

pairs = r1.join(r2, on="readName", how="inner") \
    .select(
        r1["readName"],
        r1["sequence"].alias("sequence_R1"),
        r2["sequence"].alias("sequence_R2"),
        r1["baseQualities"].alias("qual_R1"),
        r2["baseQualities"].alias("qual_R2"),
    )
```

Note that join correctness depends on read names being unique and consistent between R1 and R2 files, which is true for standard Illumina FASTQ output.

---

## Notes on the `description` field

The FASTQ header line is `@<readName> <description>`. The `description` field contains everything after the first space. For Illumina data this is typically the flow cell coordinates and filter flag (e.g. `1:N:0:ATCACG`). For many files the description is empty and the field will be null.

```python
# Count reads that passed Illumina filter (description contains ':N:')
df.filter("description LIKE '%:N:%'").count()
```
