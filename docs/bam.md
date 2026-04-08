# BAM / SAM / CRAM

BAM, SAM, and CRAM are the standard formats for storing aligned sequencing reads. All three formats share an identical Spark schema. Format detection is automatic: htsjdk reads the file magic bytes and selects the correct decoder regardless of the file extension.

- **BAM** — binary, BGZF-compressed. Indexed by `.bai`.
- **SAM** — plain text. No index possible; always reads as a single partition.
- **CRAM** — reference-compressed. Indexed by `.cram.crai`. Requires an external FASTA reference for full sequence decoding.

---

## Schema

| Field | Type | Notes |
|---|---|---|
| `readName` | String | Query template name |
| `flags` | Integer | SAM bitfield (see SAM spec §1.4.2) |
| `referenceName` | String | Reference sequence name (`RNAME`); null for unmapped reads |
| `start` | Integer | 1-based leftmost mapping position (`POS`) |
| `mappingQuality` | Integer | Mapping quality (`MAPQ`); 255 = unavailable |
| `cigar` | String | CIGAR string; `*` when unavailable |
| `mateReferenceName` | String | Reference name of the mate (`RNEXT`); null for unpaired reads |
| `mateStart` | Integer | 1-based mate position (`PNEXT`) |
| `insertSize` | Integer | Signed observed template length (`TLEN`) |
| `sequence` | String | Read bases (`SEQ`) |
| `baseQualities` | String | ASCII Phred+33 base qualities (`QUAL`) |
| `attributes` | Map(String, String) | Optional SAM tags (`NM`, `MD`, `RG`, `AS`, …); see note below |

All fields can be null for unmapped or partially-specified records.

**`attributes` column:** Each SAM optional tag (`TAG:TYPE:VALUE`) is stored as a string key and string value, e.g. `NM → "3"`, `RG → "1"`. If you select columns without including `attributes`, the tag map construction is skipped entirely on the executor — useful when attributes are large and not needed.

---

## Reading BAM files

```python
# Minimal — auto-locates co-located .bai
df = spark.read.format("bam").load("s3a://bucket/sample.bam")
df.printSchema()
df.show(5)

# Explicit index path
df = spark.read.format("bam") \
    .option("indexPath", "s3a://idx/sample.bam.bai") \
    .load("s3a://bucket/sample.bam")

# Index directory (for cohort reads)
df = spark.read.format("bam") \
    .option("indexDir", "s3a://idx/cohort/") \
    .load("s3a://bucket/cohort/")

# Disable index lookup (single partition, full scan)
df = spark.read.format("bam") \
    .option("useIndex", "false") \
    .load("s3a://bucket/sample.bam")

# Control partition count
df = spark.read.format("bam") \
    .option("numPartitions", "50") \
    .load("s3a://bucket/sample.bam")
```

### Region filtering

Apply a region filter as a standard Spark filter expression. The `referenceName` equality and `start` range are recognized by the scan builder and pushed to the BAI index so only the relevant BGZF blocks are fetched:

```python
df = spark.read.format("bam").load("s3a://bucket/sample.bam") \
    .filter("referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370")
```

When a region filter is active, the scan produces a single partition with a BAI-guided region query instead of VFO-based splitting. Spark re-applies the filter as a post-filter to handle reads that span region boundaries.

---

## Reading SAM files

SAM files are detected automatically from the `@HD` / `@SQ` header magic. Because SAM is plain text with no BGZF framing, no index is possible and reads always use a single full-file partition.

```python
df = spark.read.format("bam").load("/data/sample.sam")
```

BAI resolution is skipped for `.sam` files. The `indexPath` and `indexDir` options are accepted but silently ignored.

---

## Reading CRAM files

CRAM files use the `"cram"` format name. All other options are identical to BAM.

```python
# With reference file (required for full sequence decoding)
df = spark.read.format("cram") \
    .option("referenceFile", "s3a://ref/hg38.fa") \
    .load("s3a://bucket/sample.cram")

# Without reference (embedded bases only; some records may have null sequence)
df = spark.read.format("cram") \
    .option("referenceMode", "none") \
    .load("s3a://bucket/sample.cram")

# Region filter (pushed to CRAI index)
df = spark.read.format("cram") \
    .option("referenceFile", "s3a://ref/hg38.fa") \
    .load("s3a://bucket/sample.cram") \
    .filter("referenceName = 'chr1' AND start >= 1000000 AND start <= 2000000")
```

### CRAM reference options

| Option | Default | Description |
|---|---|---|
| `referenceFile` | — | Path to the FASTA reference. A `.fai` index must be co-located at `<referenceFile>.fai`. |
| `referenceMode` | `"file"` when `referenceFile` is set; `"none"` otherwise | `"file"` — use `referenceFile`; `"md5"` — look up sequences from ENA/NCBI by MD5 (network, not recommended on clusters); `"none"` — no reference |

The `.fai` index does not need to be in the same filesystem as the CRAM; you can store the reference on a separate S3 bucket or DBFS path.

---

## Multi-file reads (directories and globs)

When `path` is a directory or glob, the scan enumerates all matching `.bam` / `.sam` / `.cram` files and plans partitions for each independently. Results are unioned.

```python
# All BAM files in a directory
df = spark.read.format("bam") \
    .option("indexDir", "s3a://idx/cohort/") \
    .load("s3a://bucket/cohort/")

# Glob
df = spark.read.format("bam").load("s3a://bucket/cohort/*.bam")
```

Index resolution runs per-file. If some files have a BAI and others do not, files with a BAI get VFO-split partitions and files without get single-partition full scans.

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `indexPath` | — | Explicit BAI (BAM) or CRAI (CRAM) path. Applies to single-file reads only; ignored for directories and globs. |
| `indexDir` | — | Directory containing index files. Resolved as `<indexDir>/<filename>.bai` or `<indexDir>/<filename>.cram.crai` per data file. |
| `numPartitions` | `200` | Maximum partitions per file when VFO-based splitting is active. References are grouped into `min(numPartitions, numRefs)` partitions, plus one unmapped partition. Has no effect when a region filter is pushed or when no index is found. |
| `useIndex` | `true` | Set `false` to skip index resolution and force a single full-scan partition. |
| `referenceFile` | — | CRAM only. Path to FASTA reference (`.fai` must be co-located). |
| `referenceMode` | `"file"` / `"none"` | CRAM only. Controls how the CRAM decoder resolves reference sequences. |

---

## Partition planning summary

| Condition | Partitions produced |
|---|---|
| BAM + BAI, no region filter | `min(numPartitions, numRefs)` per-reference partitions + 1 unmapped partition |
| BAM + BAI + region filter | 1 partition (BAI-guided region query) |
| BAM, no BAI | 1 partition (full scan) |
| SAM (any) | 1 partition (full scan) |
| CRAM + CRAI, no region filter | 1 partition per CRAI entry |
| CRAM + CRAI + region filter | 1 partition (CRAI-guided region query) |
| CRAM, no CRAI | 1 partition (full scan) |

### BAI index resolution order

For each file in the read:

1. `indexPath` option (single-file reads only)
2. `indexDir/<filename>.bai`
3. Co-located `<bamPath>.bai`
4. No index found → single partition

### CRAI index resolution order

1. `indexPath` option (single-file reads only)
2. `indexDir/<filename>.cram.crai`
3. Co-located `<cramPath>.cram.crai`
4. No index found → single partition

---

## Column pruning

The scan builder implements `SupportsPushDownRequiredColumns`. If you select columns that do not include `attributes`, the per-row tag map construction is skipped on executors. Every other field requires reading the full `SAMRecord`, so no I/O is saved by excluding scalar columns — only the CPU cost of building the attributes map.

```python
# attributes map construction is skipped per row
df = spark.read.format("bam").load("s3a://bucket/sample.bam") \
    .select("readName", "referenceName", "start", "cigar")
```

---

## Common SAM flag values

| Flag | Hex | Meaning |
|---|---|---|
| READ_PAIRED | 0x1 | Read is paired |
| PROPER_PAIR | 0x2 | Both reads mapped in proper pair |
| READ_UNMAPPED | 0x4 | Read itself is unmapped |
| MATE_UNMAPPED | 0x8 | Mate is unmapped |
| READ_REVERSE_STRAND | 0x10 | Read is on reverse strand |
| FIRST_OF_PAIR | 0x40 | Read is first in pair |
| SECOND_OF_PAIR | 0x80 | Read is second in pair |
| SECONDARY_ALIGNMENT | 0x100 | Not primary alignment |
| READ_FAILS_VENDOR_QC | 0x200 | Failed vendor quality check |
| DUPLICATE | 0x400 | PCR or optical duplicate |
| SUPPLEMENTARY_ALIGNMENT | 0x800 | Supplementary alignment |

```python
# Filter to primary, mapped, passing-QC reads
df.filter("(flags & 0x4) = 0 AND (flags & 0x100) = 0 AND (flags & 0x200) = 0")

# First-of-pair reads only
df.filter("(flags & 0x41) = 0x41")
```
