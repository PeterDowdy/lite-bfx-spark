# BED

BED (Browser Extensible Data) files store genomic intervals — peaks, genes, regulatory elements, coverage regions, and so on. The format has a variable number of columns (BED3 through BED12) with no header declaring column count.

Supported extensions: `.bed`, `.bed.gz`. The format name is `"bed"`.

---

## Schema

All fields beyond `chromEnd` are nullable. Which fields are populated depends on the number of columns in the file.

| Field | Type | Nullable | BED columns required | Notes |
|---|---|---|---|---|
| `chrom` | String | No | BED3+ | Chromosome / contig name |
| `chromStart` | Long | No | BED3+ | Start position, **0-based inclusive** |
| `chromEnd` | Long | No | BED3+ | End position, **0-based exclusive** |
| `name` | String | Yes | BED4+ | Feature name; null in BED3 files |
| `score` | Integer | Yes | BED5+ | Score 0–1000; null in BED3/4 files |
| `strand` | String | Yes | BED6+ | `"+"`, `"-"`, or `"."` |
| `thickStart` | Long | Yes | BED7+ | Coding region start (0-based) |
| `thickEnd` | Long | Yes | BED8+ | Coding region end (0-based exclusive) |
| `itemRgb` | String | Yes | BED9+ | `"R,G,B"` color string |
| `blockCount` | Integer | Yes | BED10+ | Number of exon blocks |
| `blockSizes` | String | Yes | BED11+ | Comma-joined block sizes |
| `blockStarts` | String | Yes | BED12 | Comma-joined block starts, relative to `chromStart` |

**Coordinate system:** BED uses 0-based half-open intervals — `chromStart` is inclusive, `chromEnd` is exclusive. A one-base interval at position 100 (1-based) is stored as `chromStart=99, chromEnd=100`.

Column count is detected from the first record in each partition. Missing trailing columns produce nulls for all nullable fields.

---

## Reading BED files

### Uncompressed BED

Uncompressed BED files have no index. They are split into fixed-size byte-range
partitions: each executor seeks to its chunk boundary, discards bytes up to the
next newline to land on a clean line start, and reads records until the next line
would begin at or past its chunk end. Chunks that contain no data lines produce
zero rows. The split size is controlled by the `bedSplitSize` option (default 128 MB).

```python
df = spark.read.format("bed").load("/data/peaks.bed")
df.printSchema()
df.show(5)

# Control partition count for a large uncompressed file
df = spark.read.format("bed") \
    .option("bedSplitSize", str(32 * 1024 * 1024)) \
    .load("/data/peaks.bed")
```

### Bgzipped BED with tabix

Bgzipped `.bed.gz` files with a co-located `.tbi` or `.csi` tabix index support region queries.

```python
df = spark.read.format("bed").load("s3a://bucket/peaks.bed.gz")

# Region filter — pushed to tabix index
df.filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000").show()
```

---

## Predicate pushdown

The scan builder recognizes the following filter patterns and uses them to plan tabix-guided partitions:

| Filter expression | Pushdown effect |
|---|---|
| `chrom = '<value>'` | Only tabix blocks for that chromosome are read |
| `chrom = '<value>' AND chromStart >= A` | Blocks from position A on that chromosome |
| `chrom = '<value>' AND chromEnd <= B` | Blocks up to position B on that chromosome |
| Combined `chromStart >= A AND chromEnd <= B` | Blocks overlapping the interval |

All pushed filters are re-applied by Spark as a post-filter, so results are always correct even if an interval straddles a block boundary.

```python
# Pushed — tabix seeks to chr22 blocks only
df.filter("chrom = 'chr22'").count()

# Pushed — only blocks overlapping the region
df.filter("chrom = 'chr1' AND chromStart >= 1000000 AND chromEnd <= 2000000")

# Not pushed — no chrom filter
df.filter("chromStart >= 1000000")
```

---

## Index resolution order

For each BED file:

1. `indexPath` option (single-file reads only)
2. Co-located `<filePath>.tbi`
3. Co-located `<filePath>.csi`
4. No index → single partition, full scan

```python
# Explicit tabix path
df = spark.read.format("bed") \
    .option("indexPath", "s3a://idx/peaks.bed.gz.tbi") \
    .load("s3a://bucket/peaks.bed.gz")

# Skip index
df = spark.read.format("bed") \
    .option("useIndex", "false") \
    .load("s3a://bucket/peaks.bed.gz")
```

---

## Spark SQL

BED files can be queried directly from Spark SQL without building a `DataFrameReader`.

### Direct path reference

For reads that need no options, use the `format.\`path\`` backtick syntax:

```sql
SELECT chrom, count(*) AS intervals
FROM bed.`s3a://bucket/peaks.bed.gz`
GROUP BY chrom
ORDER BY intervals DESC;
```

### CREATE TEMPORARY VIEW (with options)

To pass options such as `indexPath` or `useIndex`, or to apply region filters via `WHERE`, create a temporary view first:

```sql
CREATE TEMPORARY VIEW peaks
USING bed
OPTIONS (
  path 's3a://bucket/peaks.bed.gz'
);

-- Region filter — pushed to tabix index when a .tbi is co-located
SELECT chrom, chromStart, chromEnd, name
FROM peaks
WHERE chrom = 'chr1'
  AND chromStart >= 1000000
  AND chromEnd   <= 2000000;
```

Region filters in the `WHERE` clause trigger the same tabix-guided partition optimization as the DataFrame API.

---

## Options reference

| Option | Default | Description |
|---|---|---|
| `indexPath` | — | Explicit tabix index path (`.tbi` or `.csi`). Single-file reads only. |
| `indexDir` | — | Directory of tabix index files. Resolved as `<indexDir>/<filename>.tbi`. |
| `bedSplitSize` | `134217728` (128 MB) | Byte size of each line-split partition for uncompressed BED files. Has no effect for `.bed.gz`. |
| `numPartitions` | `200` | Reserved for future multi-block tabix splitting. Currently one partition is planned per query. |
| `useIndex` | `true` | Set `false` to force a full-file single-partition scan for bgzipped BED. |

---

## BED column counts

BED files do not declare their column count in a header. The library reads the first record to detect column count, then applies that to all subsequent records in the partition. Mixed column counts within a single file are not supported.

```python
# BED3 file: name, score, strand, … are all null
df3 = spark.read.format("bed").load("/data/regions.bed")
df3.select("chrom", "chromStart", "chromEnd").show()

# BED6 file: name, score, strand populated; thickStart, … are null
df6 = spark.read.format("bed").load("/data/peaks_named.bed")
df6.select("chrom", "chromStart", "chromEnd", "name", "score", "strand").show()

# BED12 gene annotation: all columns populated
df12 = spark.read.format("bed").load("/data/genes.bed")
df12.select("chrom", "chromStart", "chromEnd", "name",
            "blockCount", "blockSizes", "blockStarts").show()
```

---

## Coordinate conversion

BED uses 0-based half-open coordinates; BAM/VCF use 1-based inclusive. To join BED intervals with BAM reads:

```python
from pyspark.sql.functions import col

reads = spark.read.format("bam").load("s3a://bucket/sample.bam")
peaks = spark.read.format("bed").load("s3a://bucket/peaks.bed.gz")

# Convert BED to 1-based inclusive to match BAM start
peaks_1based = peaks.withColumn("start1", col("chromStart") + 1) \
                     .withColumn("end1", col("chromEnd"))

# Reads overlapping peaks (approximate — does not account for read length)
reads.join(
    peaks_1based,
    (reads["referenceName"] == peaks_1based["chrom"]) &
    (reads["start"] >= peaks_1based["start1"]) &
    (reads["start"] <= peaks_1based["end1"]),
    "inner"
)
```

---

## Generating a tabix index

To enable region queries on a bgzipped BED file:

```bash
bgzip peaks.bed              # → peaks.bed.gz
tabix -p bed peaks.bed.gz   # → peaks.bed.gz.tbi
```

Both `bgzip` and `tabix` are included in the htslib package (same source as samtools).
