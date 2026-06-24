# BAM / SAM / CRAM

BAM, SAM, and CRAM are the standard formats for storing aligned sequencing reads. All three formats share an identical Spark schema. Format detection is automatic: htsjdk reads the file magic bytes and selects the correct decoder regardless of the file extension.

- **BAM** — binary, BGZF-compressed. Indexed by `.bai`.
- **SAM** — plain text. No index possible; split into fixed-size line-based byte-range partitions.
- **CRAM** — reference-compressed. Indexed by `.cram.crai`. Requires an external FASTA reference for full sequence decoding.

---

## Schema

| Field | Type | Notes |
|---|---|---|
| `readName` | String | Query template name |
| `flags` | Integer | SAM bitfield (see SAM spec §1.4.2); always non-null |
| `referenceName` | String | Reference sequence name (`RNAME`); null for unmapped reads |
| `start` | Long | 1-based leftmost mapping position (`POS`) |
| `mappingQuality` | Integer | Mapping quality (`MAPQ`); 255 = unavailable |
| `cigar` | String | CIGAR string; `*` when unavailable |
| `mateReferenceName` | String | Reference name of the mate (`RNEXT`); null for unpaired reads |
| `mateStart` | Long | 1-based mate position (`PNEXT`) |
| `insertSize` | Integer | Signed observed template length (`TLEN`) |
| `sequence` | String | Read bases (`SEQ`) |
| `baseQualities` | String | ASCII Phred+33 base qualities (`QUAL`) |
| `attributes` | Map(String, String) | Optional SAM tags (`NM`, `MD`, `RG`, `AS`, …); see note below |
| `start0` | Long | 0-based position (`start - 1`); null for unmapped reads. Use this when joining with BED data. |

All fields except `flags` can be null for unmapped or partially-specified records.

### Column naming

The alignment columns use descriptive names by default. Set `columnNames` to `sam` to use the
canonical SAM-spec field names instead — handy when porting queries that already speak SAM:

```python
df = spark.read.format("bam").option("columnNames", "sam").load("s3a://bucket/sample.bam")
df.select("qname", "rname", "pos", "mapq").show(5)
```

| `descriptive` (default) | `sam` |
|---|---|
| `readName` | `qname` |
| `flags` | `flag` |
| `referenceName` | `rname` |
| `start` | `pos` |
| `mappingQuality` | `mapq` |
| `cigar` | `cigar` |
| `mateReferenceName` | `rnext` |
| `mateStart` | `pnext` |
| `insertSize` | `tlen` |
| `sequence` | `seq` |
| `baseQualities` | `qual` |
| `attributes` | `attributes` *(no SAM-spec equivalent)* |
| `start0` | `start0` *(no SAM-spec equivalent)* |

Column order, types, and nullability are identical in both modes. Region-filter pushdown works
with whichever name set is active — filter on `rname`/`pos` in `sam` mode, `referenceName`/`start`
otherwise. The Scala `bamRegion`/`cramRegion` helpers always use the descriptive names.

> **Coordinate system:** `start` and `mateStart` are **1-based** as defined by the SAM spec. BED `chromStart` is **0-based**. Joining BAM and BED DataFrames directly on `start = chromStart` produces off-by-one errors. Use `start0` (which equals `chromStart` for the same position) or apply `start - 1` explicitly.

**`attributes` column:** Each SAM optional tag is stored under its two-character tag key, with the value encoded as the SAM `TYPE:VALUE` optional-field form so the type is preserved and downstream consumers can parse safely — e.g. `NM → "i:3"`, `RG → "Z:1"`, `mapping quality char → "A:P"`, `AS → "f:0.98"`, and integer arrays as `"B:i,1,2,3"`. To recover a typed value, split on the first `:` to get the SAM type code, then the remainder is the value (for `B` arrays the remainder is `subtype,v1,v2,…`). Integer subtypes (`c/C/s/S/i/I`) collapse to `i` and hex (`H`) tags are rendered as a `B` byte array, matching `samtools view` output. If you select columns without including `attributes`, the tag map construction is skipped entirely on the executor — useful when attributes are large and not needed.

### File metadata (`_metadata`)

A hidden `_metadata` struct column is available on every BAM/SAM/CRAM read, compatible with the
[Spark/Databricks file-source metadata column](https://docs.databricks.com/en/ingestion/file-metadata-column.html).
It is not part of the default schema (`SELECT *` excludes it) and must be referenced explicitly:

```python
df = spark.read.format("bam").load("s3a://bucket/sample.bam")
df.select("readName", "_metadata.file_path", "_metadata.file_size").show(5, truncate=False)
df.select("_metadata").printSchema()
```

| Field | Type | Notes |
|---|---|---|
| `file_path` | String | Full URI of the source file |
| `file_name` | String | Final path component (e.g. `sample.bam`) |
| `file_size` | Long | File length in bytes |
| `file_modification_time` | Timestamp | Last-modified time of the file |
| `index_path` | String (nullable) | Index file (BAI/CRAI) used to locate this partition's data, or null when no index was used (BGZF/SAM splits, `useIndex=false`) |

The first four fields use the same names and types as Spark's built-in `_metadata`, so existing
queries referencing them port over unchanged; `index_path` is an added, lite-bfx-spark-specific
field. The `file_block_start`, `file_block_length`, and `row_index` sub-fields of Spark's built-in
column are not exposed: byte ranges and row indices have no stable meaning under genomic
(per-reference / per-container) partitioning.

The `_metadata` column is available on **every** lite-bfx-spark format (BAM/SAM/CRAM, FASTA,
FASTQ, BED, VCF/BCF) with the same shape. `index_path` reflects the format's index: BAI/CRAI for
BAM/CRAM, FAI for FASTA, tabix/CSI for BED/VCF, and always null for FASTQ (which has no index).

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

# Disable index lookup (BGZF-split partitions, no BAI seek)
df = spark.read.format("bam") \
    .option("useIndex", "false") \
    .load("s3a://bucket/sample.bam")

# Control partition count
df = spark.read.format("bam") \
    .option("numPartitions", "50") \
    .load("s3a://bucket/sample.bam")
```

### Region filtering and predicate pushdown

Apply a region filter as a standard Spark `.filter()` expression. The scan builder recognizes specific filter patterns and uses them to guide BAI-based partition planning; Spark always re-applies all filters as a post-filter pass for correctness.

**Recognized filter patterns (BAM + BAI only):**

| Filter expression | Effect |
|---|---|
| `referenceName = '<value>'` | Required anchor — enables any pushdown |
| `start >= N` or `start > N` | Lower bound on alignment start (1-based) |
| `start <= N` or `start < N` | Upper bound on alignment start (1-based) |
| `referenceName = '<v>' AND start >= A AND start <= B` | Single BAI-guided partition covering `[A, B]` |

`referenceName` equality **must** be present for any pushdown to occur. A `start` range without `referenceName` is not pushable and results in a full scan. For SAM files (plain text, no BGZF), filters are always applied as a Spark post-filter — no BAI-guided optimization is possible regardless of the filter expression.

```python
# Pushed — single partition, BAI-guided byte-range read
df = spark.read.format("bam").load("s3a://bucket/sample.bam") \
    .filter("referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370")

# Pushed — referenceName only (no start range)
df.filter("referenceName = 'chrX'")

# Not pushed — no referenceName; full scan with post-filter
df.filter("start >= 43044295 AND start <= 43125370")
```

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
| `numPartitions` | `200` | Maximum partitions per file. For BAM + BAI: one partition per reference (large references are sub-split, see `indexedSplitSize`) plus one unmapped partition; when a file has more references than `numPartitions`, references are grouped — heavy references are still sub-split and the rest are bin-packed by BAI byte span into the remaining budget (skew-aware), not chopped into equal-count slices. For CRAM (with or without CRAI): containers are grouped into `min(numPartitions, numContainers)` partitions. Unindexed BAM files use `bgzfSplitSize` instead. |
| `indexedSplitSize` | `134217728` (128 MB) | Target compressed byte size per partition for **indexed** BAM reads (BAI present). A reference, or a pushed region, whose BAI byte span exceeds this is divided into multiple partitions split on BAI record-start virtual file offsets (exact — no records dropped or duplicated). Lower it to increase parallelism on files with few, large references. |
| `bgzfSplitSize` | `134217728` (128 MB) | Byte size of each BGZF-split partition for unindexed BAM files. Has no effect when a BAI index is found or when `useIndex` is false. |
| `samSplitSize` | `134217728` (128 MB) | Byte size of each line-split partition for SAM files. Has no effect for BAM or CRAM. |
| `useIndex` | `true` | Set `false` to skip index resolution. For BAM this forces the BGZF-split path (multiple byte-range partitions, no BAI seek). For CRAM this falls back to container header scanning (still multi-partition, but without seeking via CRAI). |
| `columnNames` | `"descriptive"` | Alignment column names. `"sam"` uses canonical SAM-spec field names (`qname`, `flag`, `rname`, `pos`, `mapq`, `rnext`, `pnext`, `tlen`, `seq`, `qual`); see [Column naming](#column-naming). |
| `referenceFile` | — | CRAM only. Path to FASTA reference (`.fai` must be co-located). |
| `referenceMode` | `"file"` / `"none"` | CRAM only. Controls how the CRAM decoder resolves reference sequences. |

---

## Partition planning summary

| Condition | Partitions produced |
|---|---|
| BAM + BAI, no region filter | One partition per reference (references larger than `indexedSplitSize` are sub-split on BAI VFOs) + 1 unmapped partition; when references outnumber `numPartitions`, heavy references stay sub-split and the rest are bin-packed by BAI byte span (skew-aware) |
| BAM + BAI + region filter | The region's BAI span split into `~ceil(spanBytes / indexedSplitSize)` VFO partitions (1 for a small region) |
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
