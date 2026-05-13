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
| `start0` | Long | 0-based position (`start - 1`); null for unmapped reads. Use this when joining with BED data. |

All fields can be null for unmapped or partially-specified records.

> **Coordinate system:** `start` and `mateStart` are **1-based** as defined by the SAM spec. BED `chromStart` is **0-based**. Joining BAM and BED DataFrames directly on `start = chromStart` produces off-by-one errors. Use `start0` (which equals `chromStart` for the same position) or apply `start - 1` explicitly.

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

SAM files are detected automatically from the `@HD` / `@SQ` header magic. Because SAM is plain text with no BGZF framing, no index is possible. SAM files are split into fixed-size byte-range partitions: each executor seeks to its chunk boundary, discards bytes up to the next newline to land on a clean line start, and reads records until the next line would begin at or past its chunk end. Chunks that contain no data lines produce zero rows. The split size is controlled by the `samSplitSize` option (default 128 MB).

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

Index resolution runs per-file. If some files have a BAI and others do not, files with a BAI get VFO-split partitions and files without get BGZF-split partitions.

---

## Spark SQL

BAM, SAM, and CRAM files can be queried directly from Spark SQL without building a `DataFrameReader`.

### Direct path reference

For reads that need no options, use the `format.\`path\`` backtick syntax:

```sql
SELECT referenceName, count(*) AS reads
FROM bam.`s3a://bucket/sample.bam`
GROUP BY referenceName
ORDER BY reads DESC;
```

### CREATE TEMPORARY VIEW (with options)

To pass options — such as `indexPath`, `referenceFile`, or `numPartitions` — create a temporary view first, then query it with standard SQL including `WHERE` clauses for region filtering:

```sql
-- BAM with explicit index
CREATE TEMPORARY VIEW reads
USING bam
OPTIONS (
  path      's3a://bucket/sample.bam',
  indexPath 's3a://idx/sample.bam.bai'
);

SELECT referenceName, start, cigar
FROM reads
WHERE referenceName = 'chr17'
  AND start >= 43044295
  AND start <= 43125370;

-- CRAM with reference (referenceFile is required for full sequence decoding)
CREATE TEMPORARY VIEW cram_reads
USING cram
OPTIONS (
  path          's3a://bucket/sample.cram',
  referenceFile 's3a://ref/hg38.fa'
);

SELECT readName, referenceName, start FROM cram_reads LIMIT 10;
```

Region filters in the `WHERE` clause trigger the same BAI/CRAI-guided partition optimization as the DataFrame API.

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `indexPath` | — | Explicit BAI (BAM) or CRAI (CRAM) path. Applies to single-file reads only; ignored for directories and globs. |
| `indexDir` | — | Directory containing index files. Resolved as `<indexDir>/<filename>.bai` or `<indexDir>/<filename>.cram.crai` per data file. |
| `numPartitions` | `200` | Maximum partitions per file for index-guided splitting. For BAM + BAI: references are grouped into `min(numPartitions, numRefs)` partitions plus one unmapped partition. For CRAM (with or without CRAI): containers are grouped into `min(numPartitions, numContainers)` partitions. Has no effect when a region filter is pushed. Unindexed BAM files use `bgzfSplitSize` instead. |
| `bgzfSplitSize` | `134217728` (128 MB) | Byte size of each BGZF-split partition for unindexed BAM files. Has no effect when a BAI index is found or when `useIndex` is false. |
| `samSplitSize` | `134217728` (128 MB) | Byte size of each line-split partition for SAM files. Has no effect for BAM or CRAM. |
| `useIndex` | `true` | Set `false` to skip index resolution. For BAM this forces a single BGZF-split scan. For CRAM this falls back to container header scanning (still multi-partition, but without seeking via CRAI). |
| `referenceFile` | — | CRAM only. Path to FASTA reference (`.fai` must be co-located). |
| `referenceMode` | `"file"` / `"none"` | CRAM only. Controls how the CRAM decoder resolves reference sequences. |

---

## Partition planning summary

| Condition | Partitions produced |
|---|---|
| BAM + BAI, no region filter | `min(numPartitions, numRefs)` per-reference partitions + 1 unmapped partition |
| BAM + BAI + region filter | 1 partition (BAI-guided region query) |
| BAM, no BAI | `ceil(fileSize / bgzfSplitSize)` BGZF-split partitions (default split: 128 MB) |
| SAM, no region filter | `ceil(fileSize / samSplitSize)` line-split partitions (default split: 128 MB) |
| SAM + region filter | `ceil(fileSize / samSplitSize)` line-split partitions; each worker applies the region filter independently |
| CRAM + CRAI, no region filter | `min(numPartitions, numContainers)` container-split partitions |
| CRAM + CRAI + region filter | 1 partition (CRAI-guided region query) |
| CRAM, no CRAI | `min(numPartitions, numContainers)` container-split partitions (header scan) |

### BAI index resolution order

For each file in the read:

1. `indexPath` option (single-file reads only)
2. `indexDir/<filename>.bai`
3. Co-located `<bamPath>.bai`
4. No index found → BGZF block-level splitting (see partition planning table)

### CRAI index resolution order

1. `indexPath` option (single-file reads only)
2. `indexDir/<filename>.cram.crai`
3. Co-located `<cramPath>.cram.crai`
4. No index found → container header scan (multiple partitions, same `numPartitions` cap)

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
