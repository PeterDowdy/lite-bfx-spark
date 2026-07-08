# lite-bfx-spark (Python)

Pure-Python [Apache Spark 4](https://spark.apache.org/) **Python Data Source** readers for
genomics file formats — BAM, SAM, CRAM, FASTQ, VCF, BCF, FASTA, and BED. A JAR-free,
`pip`-installable equivalent of the [Java DataSource V2
library](https://github.com/PeterDowdy/lite-bfx-spark): parsing is done with
[pysam](https://pysam.readthedocs.io/) (bundled htslib), and the DataFrame schemas match
the JAR field-for-field.

> **Status: alpha.** See [`TASKS.md`](TASKS.md) for what is implemented and
> [`../docs/proposals/python-data-source.md`](../docs/proposals/python-data-source.md) for
> the design and validation.

## Install

```bash
pip install lite-bfx-spark
```

`pysam` is the only hard dependency; `pyspark>=4.0` is expected from the Spark runtime
(cluster/serverless), exactly like the JAR's `provided`-scope Spark dependency.

## Usage

```python
import litebfx
litebfx.register_all(spark)          # registers bam, cram, vcf, fasta, fastq, bed

df = (spark.read.format("bam")
        .load("/Volumes/genomics/core/aligned/sample.bam")
        .filter("referenceName = 'chr1'"))

# region-pruned read (works on Spark 4.0 via the explicit option; also via .filter on 4.1+)
reads = (spark.read.format("bam")
           .option("region", "chr1:1000000-2000000")
           .load("/Volumes/genomics/core/aligned/sample.bam"))
```

`.load()` also accepts a **glob**, a **directory**, or several paths — each file contributes
its own partitions:

```python
spark.read.format("bam").load("/Volumes/cohort/*.bam")     # glob
spark.read.format("fastq").load("/Volumes/run/lane1/")      # directory of FASTQs
```

### Options

| Option | Formats | Description |
|---|---|---|
| `region` | BAM, CRAM, VCF, BED | `chr1:1000-2000` — index-pruned read (also via `.filter(...)` on Spark 4.1+) |
| `indexPath` | BAM, CRAM, VCF, BED, FASTA | explicit index path (single-file reads) |
| `indexDir` | BAM, CRAM, VCF, BED, FASTA | directory of indexes, resolved as `<dir>/<file><ext>` |
| `useIndex` | BAM, CRAM, VCF, BED | `false` to ignore indexes |
| `referenceFile` | CRAM | reference FASTA (required to decode) |
| `numPartitions` | BAM, CRAM, VCF, FASTQ | max index-guided partitions (default 200) |
| `bgzfSplitSize` | BAM | byte-chunk size for unindexed BAM splits (default 128 MB) |
| `minSplitBytes` | FASTQ | min partition size for uncompressed FASTQ splits (default 64 MB) |
| `columnNames` | BAM, CRAM | `sam` for canonical SAM field names |
| `metadata` | all | `true` adds a visible `_metadata` struct (`file_path`, `file_name`, `file_size`, `file_modification_time`, `index_path`) |

## Cloud storage = a filesystem mount

This package reads **local filesystem paths only** and has no cloud SDK or credential
handling. Object storage is reached through a FUSE mount:

- **On Databricks**, use a **Unity Catalog Volume** path (`/Volumes/<cat>/<schema>/<vol>/…`,
  works on classic and serverless) or a `dbfs:/…` path — Databricks does the cloud I/O and
  auth under the mount. `dbfs:/` is normalized to `/dbfs/`.
- **Off Databricks**, mount the bucket yourself (`mountpoint-s3`, `s3fs`, `gcsfuse`,
  `blobfuse2`, `rclone mount`) and pass the mounted path.

Raw `s3://` / `abfss://` / `gs://` URIs are rejected with guidance, because their
credentials are not visible to a Python worker.

## Relationship to the JAR

The JAR and this package are two implementations of the same reader for two deployment
models — install **one**, not both. Use the JAR when you need optimizer integration
(statistics, ordering), byte-balanced splitting, or Hadoop-native access to every cloud
scheme; use this package when you want a `pip install` with no JVM library to provision.

## Supported formats

| Format | `format()` | Index | Notes |
|---|---|---|---|
| BAM | `bam` | `.bai` | per-contig or region read; unindexed BAM is split by BGZF byte range |
| SAM | `bam` | — | single partition |
| CRAM | `cram` | `.cram.crai` | needs `referenceFile`; unindexed sorted CRAM → a CRAI is synthesized |
| VCF/BCF | `vcf` | `.tbi`/`.csi` | full scan or tabix region query |
| FASTA | `fasta` | `.fai` | one row per contig |
| FASTQ | `fastq` | — | uncompressed split by byte range; `readNumber` from the R1/R2 filename |
| BED | `bed` | `.tbi`/`.csi` | BED3–BED12 |

All formats support multi-file/glob/directory reads, the `_metadata` option, and emit Arrow
`RecordBatch`es for columnar transfer.

### BED number handling (matches the JAR)

samtools never truncates BED numeric columns, so neither reader does: a numeric column
(`score`, `thickStart`, `thickEnd`, `blockCount`) takes a **clean integer or null** — a
decimal such as narrowPeak's `19.76368` (or `818.0`) becomes **null**, not `19`. The Java
reader was updated to match (it previously truncated at the dot). The remaining column
conventions are shared by both readers: absent-markers (`name`/`strand` = `.`, `itemRgb` =
`0`) → null, a packed-integer `itemRgb` → `"R,G,B"`, trailing commas stripped from block
fields, and a record with a non-integer `chromStart`/`chromEnd` dropped (as samtools drops a
malformed region).

## Development

```bash
cd python
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[test]'
pytest -q -m "not spark"     # pure-Python units (no JVM)
pytest -q                    # + Spark-backed parity tests (needs JDK 17)
```

Or run everything in Docker (JDK 17 + `samtools` for the parity oracle):

```bash
docker compose run --rm python-test        # from the repo root
```

Value parity is checked against independent oracles — `samtools view` for BAM/SAM (see
`tests/test_samtools_parity.py`), pysam / direct parse for the others — and schemas against
Java-derived field triples.
```
