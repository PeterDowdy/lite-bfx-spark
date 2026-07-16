# Proposal: a pure-Python DataSource equivalent

| | |
|---|---|
| **Status** | Draft / RFC — not yet implemented |
| **Author** | (proposal) |
| **Target** | New top-level `python/` module, published to PyPI as `lite-bfx-spark` |
| **Depends on** | Spark 4.0+ Python Data Source API, `pysam`, `pyarrow` |
| **Tracking branch** | `python_data_source` |

## Summary

Reimplement the genomics readers (BAM, SAM, CRAM, FASTQ, VCF, BCF, FASTA, BED)
against Spark 4.0's **Python Data Source API** so the library can be installed with
`pip install lite-bfx-spark` and used with **no JAR, no JVM library, and no Maven/JDK
build**. Parsing moves from htsjdk (JVM) to **pysam** (which bundles htslib). The
Python source registers under the same `format()` short names and produces
byte-for-byte-identical schemas, so it is a drop-in replacement in JAR-free
environments (Spark Connect, locked-down serverless, ad-hoc PySpark).

This is a genuine reimplementation, not a binding. The Python Data Source API is a
different, smaller surface than Java DataSource V2, and pysam is a different parser
than htsjdk. The bulk of this document is about where the two diverge and what we
lose, gain, or must rebuild.

## Motivation

The core JAR **already works from PySpark today** — `spark.read.format("bam")` resolves
to the Java `BamDataSource` once the JAR is on the classpath. So why build a Python
version at all?

1. **Zero-JAR deployment.** Attaching a JAR requires cluster-admin access, a Maven
   coordinate or an uploaded artifact, and a cluster restart. `pip install` needs none
   of that. On managed notebooks, ephemeral clusters, and CI, a pip dependency is far
   cheaper to introduce than a cluster library.
2. **Spark Connect / serverless.** On Databricks Serverless and Spark Connect clients,
   JAR attachment is restricted or awkward (UC Volumes only, no `--jars`), whereas
   Python packages install normally. A pure-Python data source runs in the Python
   worker and sidesteps JAR provisioning entirely.
3. **No JVM build toolchain.** Contributors can iterate with `pytest` and a local
   `pip install -e .` instead of Docker + Maven + a from-source samtools build.
4. **pysam is the reference parser.** Most of the Python bioinformatics ecosystem
   already speaks pysam/htslib. Interop (custom UDFs, downstream tools) is more natural
   when the reader and the ecosystem share a parser.

Non-motivation: this does **not** replace the JAR. The JAR remains the right choice
when you need the optimizer integrations (statistics, ordering), Hadoop-native cloud
I/O across every scheme, or maximum throughput. The two are complementary; see
[Coexistence and naming](#coexistence-and-naming).

## Background: the Spark Python Data Source API

Spark 4.0 shipped a first-class Python Data Source API (`pyspark.sql.datasource`). It
lets you implement a custom source in pure Python with a much smaller surface than Java
DataSource V2:

```python
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

class MyDataSource(DataSource):
    @classmethod
    def name(cls) -> str: ...          # short format() name
    def schema(self): ...              # StructType or DDL string
    def reader(self, schema) -> DataSourceReader: ...

class MyReader(DataSourceReader):
    def partitions(self) -> list[InputPartition]: ...   # runs on the driver
    def read(self, partition):                          # runs on Python workers
        # yield tuples, Row objects, or pyarrow.RecordBatch
        ...

spark.dataSource.register(MyDataSource)
df = spark.read.format("my").load("...")
```

The mental model maps cleanly onto what this library already does, but several DSv2
capabilities have **no equivalent** in the Python API. The mapping:

| Java DataSource V2 (this repo) | Python Data Source API |
|---|---|
| `BamDataSource` (`TableProvider` + `DataSourceRegister`) + `META-INF/services` | `class BamDataSource(DataSource)` + `spark.dataSource.register(...)` |
| `BamTable` (`SupportsRead`) | folded into `DataSource` |
| `BamScanBuilder` — `SupportsPushDownV2Filters` | `DataSourceReader.pushFilters()` — **Spark 4.1+ only** |
| `BamScanBuilder` — `SupportsPushDownLimit` | **no equivalent** |
| `BamScanBuilder` — `SupportsPushDownRequiredColumns` | **no equivalent** (reader always emits full schema) |
| `BamScan.planInputPartitions()` (driver) | `DataSourceReader.partitions()` (Python driver) |
| `BamInputPartition` (`Serializable` + `SerializableConfiguration`) | `InputPartition` subclass (pickled) |
| `BamPartitionReaderFactory` | implicit |
| `BamPartitionReader` (`PartitionReader<InternalRow>`) | `DataSourceReader.read()` → tuples / `Row` / `pyarrow.RecordBatch` |
| `SupportsReportStatistics` | **no equivalent** |
| `SupportsReportOrdering` | **no equivalent** |
| `HadoopSeekableStream` over `FSDataInputStream` | pysam opens a **local / FUSE-mounted path** (see Cloud I/O) |
| `SerializableConfiguration` (auto credential propagation) | **not needed** — credentials live in the FUSE mount, not the reader |
| htsjdk 4.x (shaded) | pysam (bundled htslib) |

The three "no equivalent" rows are the substance of this proposal, together with the
filesystem-only I/O model (see Cloud I/O). Everything else is mechanical.

## Goals

- **Schema parity.** Identical column names, types, nullability, and coordinate
  conventions to the Java readers, including the `columnNames=sam` alias set for BAM
  and the SAM `TYPE:VALUE` encoding of the `attributes` map. A user swapping
  implementations sees the same DataFrame.
- **Index-aware region reads.** BAI/CRAI/tabix/FAI drive partition planning and
  region queries so a filtered read on a large indexed file does not scan the whole
  file — the same value proposition as the JAR, delivered through pysam's `.fetch()`.
- **Filesystem-only I/O.** The reader takes a local path and hands it to pysam. Cloud
  storage is reached through a FUSE mount — Databricks Volumes/DBFS on the target,
  user-provided (`mountpoint-s3`, `gcsfuse`, `blobfuse2`, …) elsewhere. No cloud SDK or
  credential code lives in the library.
- **Drop-in `format()` names.** `spark.read.format("bam")` etc., matching the JAR.

## Non-goals (initially)

- Optimizer integration (statistics, ordering, limit pushdown) — the Python API
  exposes no hooks for these. Documented as a permanent gap, not a TODO.
- The hidden `_metadata` struct column — the Python API cannot contribute hidden
  metadata columns; see [Gaps](#the-_metadata-column).
- Write support. Readers only, matching the JAR.
- Cloud credential handling. The reader takes filesystem paths only; object storage is
  reached through a FUSE mount (see Cloud I/O below). No SDK or credential code ships in
  the library.
- Byte-balanced region splitting. Indexed reads use coordinate splitting (approximate
  balance, by choice) rather than byte-balanced VFO splitting. Unindexed **BAM** *is* split
  (spike-validated); unindexed **CRAM** splitting was evaluated and found feasible (see the
  Splitting section) — a candidate, not a non-goal.
- Windows **executors**. pysam has no Windows wheels; Spark executors are Linux in
  every supported deployment, so this only affects a Windows _driver_ doing local reads.

## Architecture

### Package layout

```
python/
├── pyproject.toml                 # package: lite-bfx-spark; deps: pysam, pyarrow, pyspark>=4.0
├── src/litebfx/
│   ├── __init__.py                # register_all(spark) convenience + version
│   ├── schemas.py                 # StructTypes mirroring the Java *Schema classes
│   ├── arrow.py                   # StructType -> pyarrow schema; columnar batch builders
│   ├── io.py                      # path normalization (dbfs:/ -> /dbfs/); reject raw cloud URIs
│   ├── regions.py                 # pushFilters() parsing -> (contig, start, end)
│   ├── bam.py                     # BamDataSource + BamReader (also SAM)
│   ├── cram.py                    # CramDataSource + CramReader (reference handling)
│   ├── vcf.py                     # VcfDataSource + VcfReader (VCF/BCF)
│   ├── fasta.py                   # FastaDataSource + FastaReader
│   ├── fastq.py                   # FastqDataSource + FastqReader
│   └── bed.py                     # BedDataSource + BedReader
└── tests/
    ├── conftest.py                # local SparkSession; registers sources
    ├── test_parity_*.py           # golden parity vs. the JAR (see Testing)
    └── resources -> ../../core/src/test/resources   # reuse existing fixtures
```

### Registration and usage

```python
import litebfx
litebfx.register_all(spark)   # registers bam, cram, vcf, fasta, fastq, bed

df = spark.read.format("bam").load("sample.bam") \
        .filter("referenceName = 'chr1' AND start >= 1_000_000 AND start <= 2_000_000")
```

`register_all` calls `spark.dataSource.register(...)` for each `DataSource` subclass.
We keep the short names identical to the JAR so example code and docs are shared.

### Parsing: pysam → schema mapping

pysam covers every format htsjdk does, with a bundled htslib (no system samtools
required):

| Format | pysam entry point | Region query |
|---|---|---|
| BAM / SAM / CRAM | `pysam.AlignmentFile` | `.fetch(contig, start, end)` via BAI/CRAI |
| VCF / BCF | `pysam.VariantFile` | `.fetch(contig, start, end)` via tabix/CSI |
| FASTA | `pysam.FastaFile` | `.fetch(contig)` via FAI (whole-contig random access) |
| FASTQ | `pysam.FastxFile` | streaming only (no index) |
| BED (bgzipped) | `pysam.TabixFile(parser=pysam.asBed())` | `.fetch(contig, start, end)` via tabix/CSI |
| BED (plain text) | line reader | — |

Record → row mapping reuses htsjdk-equivalent fields (`AlignedSegment.query_name`,
`.flag`, `.reference_name`, `.reference_start` (0-based → `+1` for `start`, kept as-is
for `start0`), `.mapping_quality`, `.cigarstring`, `.next_reference_name`,
`.next_reference_start`, `.template_length`, `.query_sequence`, `.qual`, and `.tags`
formatted into the SAM `TYPE:VALUE` map). The one subtlety worth pinning down in tests:
pysam exposes 0-based `reference_start`; the Java reader emits 1-based `start` and
0-based `start0`. We must reproduce both exactly.

### Schemas (parity is mandatory)

`schemas.py` mirrors the Java `*Schema` classes field-for-field. For example, BAM:

```python
from pyspark.sql.types import *

def bam_schema(sam_names: bool = False) -> StructType:
    n = (lambda descriptive, sam: sam if sam_names else descriptive)
    return StructType([
        StructField(n("readName", "qname"),          StringType(),  True),
        StructField(n("flags", "flag"),              IntegerType(), False),
        StructField(n("referenceName", "rname"),     StringType(),  True),
        StructField(n("start", "pos"),               LongType(),    True),   # 1-based
        StructField(n("mappingQuality", "mapq"),     IntegerType(), True),
        StructField("cigar",                          StringType(),  True),
        StructField(n("mateReferenceName", "rnext"), StringType(),  True),
        StructField(n("mateStart", "pnext"),         LongType(),    True),   # 1-based
        StructField(n("insertSize", "tlen"),         IntegerType(), True),
        StructField(n("sequence", "seq"),            StringType(),  True),
        StructField(n("baseQualities", "qual"),      StringType(),  True),
        StructField("attributes", MapType(StringType(), StringType(), True), True),
        StructField("start0",                         LongType(),    True),   # 0-based
    ])
```

VCF, BED, FASTA, and FASTQ schemas mirror `VcfSchema`, `BedSchema`, `FastaSchema`,
`FastqSchema` identically. A parity unit test asserts
`python_schema.json() == jar_schema.json()` for every format (the JAR schema JSON is
checked in as a golden file), so drift fails CI.

### Partition planning per format

`partitions()` runs on the driver and returns picklable `InputPartition` objects. The
per-format strategy tracks the Java planner, adapted to what pysam exposes:

| Format | Indexed, no region | Region filter | Unindexed |
|---|---|---|---|
| BAM/CRAM | group references ≤ `numPartitions`; each partition `fetch`es its contigs; final partition for unplaced reads (`fetch(contig="*")`) | single `fetch(contig, start, end)` partition | **BAM:** BGZF byte-range splits (executor-side block + record guesser). **CRAM:** single partition initially (feasible to split — see evaluation) |
| VCF/BCF | group index contigs ≤ `numPartitions` | single region partition | single partition |
| FASTA | one partition per FAI contig | single contig partition | single partition |
| FASTQ | — (no index) | — | single partition |
| BED | bgzip+tabix: group contigs | single region partition | single partition |

Large single contigs can be split further by coordinate sub-range (approximate balance is
accepted); unindexed **BAM** is split by BGZF byte range with an executor-side
record-boundary guesser. See the Splitting section below.

```python
@dataclass
class ContigGroup(InputPartition):
    contigs: list[str]
    region: tuple[int, int] | None = None    # (start, end) when a filter is pushed

class BamReader(DataSourceReader):
    def partitions(self):
        with pysam.AlignmentFile(self._fs_path(self.path)) as af:
            live = [s.contig for s in af.get_index_statistics() if s.mapped or s.unmapped]
        groups = _round_robin(live, self.num_partitions)
        parts = [ContigGroup(g, self.region) for g in groups]
        parts.append(ContigGroup(["*"]))   # unplaced reads
        return parts

    def read(self, part: ContigGroup):
        import pyarrow as pa
        with pysam.AlignmentFile(self._fs_path(self.path)) as af:
            batch = _BamBatchBuilder(self.arrow_schema)
            for contig in part.contigs:
                it = (af.fetch(contig, *part.region) if part.region
                      else af.fetch(contig))
                for rec in it:
                    batch.add(rec)
                    if batch.full():
                        yield batch.flush()
            if batch.nonempty():
                yield batch.flush()
```

### Record decoding → Arrow

`read()` should yield **`pyarrow.RecordBatch`** objects, not per-row tuples. Columnar
batches (e.g. 10k rows) avoid per-row Python→JVM serialization overhead and are the
documented fast path for Python data sources. `arrow.py` derives a `pyarrow.Schema`
from the Spark `StructType` once, and `_BamBatchBuilder` accumulates column arrays.
This is the single biggest performance lever available to a Python data source and
should be in from Phase 1, not retrofitted.

## Hard problems and gaps vs. the JAR

This is where a Python port stops being mechanical. None of these are blockers, but
each is a real difference users must understand.

### Cloud I/O: native S3/GCS, download-then-open Azure, FUSE for the rest

**Historical note:** this section originally argued the reader should open local/FUSE-mounted
paths only, on the reasoning that Python Data Source worker processes don't inherit Spark's
Hadoop config and "pysam/htslib doesn't understand those schemes." The second half of that
claim turned out to be wrong for S3 and GCS — empirically verified by binary inspection and
live reads against MinIO/fake-gcs-server/Azurite through the real Spark pipeline, not just
assumed. What follows is the design actually shipped; see `litebfx/io.py`, `_cloudfs.py`, and
`_cloud.py` for the implementation, and `python/README.md`'s "Cloud storage" section for the
user-facing version of this.

**S3 and GCS are opened directly — pysam's bundled htslib has a native remote-read backend
for both.** `hfile_s3.c` does full SigV4 v2/v4 signing (including session-token support,
needed for Databricks-vended temporary credentials) and reads `AWS_ACCESS_KEY_ID`/
`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`/`AWS_DEFAULT_REGION` directly; `hfile_gcs.c` reads
`GCS_OAUTH_TOKEN`. `spark.read.format("bam").load("s3://bucket/sample.bam")` works with the
same index-guided range-request behavior as a local file — no credential code needed in the
common case, since htslib resolves ambient credentials itself. One real wrinkle: htslib's S3
scheme recognizes only `s3://`/`s3+http://`/`s3+https://` (`s3a://`/`s3n://` give "Protocol
not supported"), and a non-AWS endpoint (MinIO, on-prem) needs the URL rewritten to
`s3+http://`/`s3+https://` *and* `HTS_S3_HOST` set — plain `s3://` with only `HTS_S3_HOST` set
still targets real AWS. `_cloud.py`'s `effective_path()`/`prepare_env()` handle this
transparently, driven by the standard `AWS_ENDPOINT_URL` env var. GCS has no such override at
all (confirmed: hardcoded `.googleapis.com`) — a real upstream htslib gap, not a litebfx one;
see `python/TASKS.md`'s open questions for the contribution opportunity this represents.

**Azure is opened directly too, but through download-then-open, not a native backend.**
htslib has no Azure support whatsoever (confirmed absent from the binary). Since pysam also
can't accept a Python-mediated byte stream for seek-capable reads (it requires a real OS file
descriptor — confirmed: it rejects `io.BytesIO`), there's no cheap bridge for range-request-only
access the way S3/GCS get. Instead, `pyarrow.fs.AzureFileSystem` downloads the object (and its
index, if any) to a local temp file, and pysam opens that local copy. This trades away the
range-request efficiency story for Azure specifically — the whole file transfers once per
worker process, regardless of query selectivity — in exchange for direct `abfss://` support
with no mount. Query efficiency *after* the download is unaffected (pysam's own indexed
seeking works normally against the local copy).

**On Databricks Runtime 18.0+ / Serverless client 5+, credential vending is automatic — no
extra install step.** `_cloud.py` vends a short-lived, path-scoped credential from Unity
Catalog's Temporary Credentials API (`generate_temporary_path_credentials`) for any cloud
path, lazily on first open in a worker process, and it takes priority over ambient
credentials. This relies on `databricks-sdk`, which is *not* a litebfx extra (there was one
briefly; it was removed after a real install failure on dedicated compute — see
`pyproject.toml`'s comment and `python/TASKS.md`) — `databricks-sdk` is preinstalled on every
Databricks Runtime/Serverless image, and declaring it as a dependency just forces pip to
reconcile against whatever version is already pinned there. This is the Databricks-native
equivalent of what UC Volumes' FUSE mount does internally, applied directly to
`s3://`/`gs://`/`abfss://` paths instead of requiring the FUSE indirection.

**The version floor is real, not conservative padding**: inspecting the actual bundled SDK
wheels (not guessing) found that `generate_temporary_path_credentials` doesn't exist at all
in Runtime 17.3 LTS's databricks-sdk (0.49.0) or earlier — only a table-scoped equivalent
that needs a UC `table_id`, unusable for arbitrary paths. `_cloud.uc_path_vending_warning()`
detects this (parsing `DATABRICKS_RUNTIME_VERSION`'s two independent schemes: classic DBR's
plain numeric version, and Serverless's separate `client.N.M` versioning) and
`register_all()` warns once at setup time; `_cloud._import_path_credentials_api()` is the
actual-import-based check `vend_credential()` itself relies on, so a stale or wrong version
inference in the warning never causes an incorrect *behavior*, only a possibly-missing or
spurious warning. Below the threshold, direct cloud reads fall back to ambient credentials —
fine on a classic cluster with an instance profile, a real gap on Serverless (no
instance-profile equivalent) below client 5.

One thing this *still doesn't* resolve without a real workspace, even where the version floor
is met: whether `WorkspaceClient()`'s default auth actually resolves inside the isolated
Python Data Source worker subprocess (vs. only the driver, which is well-established to
work) — see `tests/smoke_uc_credential_vending.py` and `TASKS.md`'s open questions.

**Off Databricks, `pip install lite-bfx-spark` alone covers S3 and Azure; GCS needs the `gcp`
extra.** `pysam` is the only hard dependency — `pyspark`/`pyarrow` are expected to already be
present (the Spark runtime), matching the JAR's `provided`-scope dependency. GCS is a partial
exception to "ambient credentials just work": htslib's native GCS backend needs an
already-minted OAuth access token in `GCS_OAUTH_TOKEN`, not the `GOOGLE_APPLICATION_CREDENTIALS`
key-file path most GCS users actually have ambiently. `pip install "lite-bfx-spark[gcp]"`
mints one automatically from that key file (cached, refreshed near expiry) — see `_cloud.py`'s
`_mint_ambient_gcs_token()`. S3 and Azure don't have this gap; their ambient credential shapes
are exactly what htslib/`pyarrow.fs` already expect.

**FUSE mounts still matter for everything else.** `adl://`, `hdfs://`, `http(s)://`, `ftp://`
have no native-or-download story and are still rejected with guidance to mount instead — the
original reasoning in this section (Databricks exposes cloud storage as FUSE-mounted POSIX
paths via UC Volumes or DBFS; off Databricks, bring your own `mountpoint-s3`/`gcsfuse`/
`blobfuse2` mount) still applies exactly as written for those schemes.

!!! warning "FUSE random-access performance is still an open question, now scoped narrower"
    The index → range-request I/O reduction over a FUSE mount only materializes if the mount
    turns pysam's seek-heavy `.fetch()` into tight range reads rather than streaming or
    whole-file local caching — this was never benchmarked, and remains unverified. It no
    longer affects S3/GCS (native reads, verified via real Range-request behavior against
    MinIO/fake-gcs-server through the actual Spark pipeline) or Azure (download-then-open has
    a different, already-understood performance story: one full transfer, then local-speed
    reads). It's now purely about the schemes that still require a mount — UC Volumes for
    `adl://`/`hdfs://`-style access, or a self-managed FUSE driver for anything else.

### Filter pushdown is Spark 4.1+ and narrower

`DataSourceReader.pushFilters()` exists only in **Spark 4.1+**. On 4.0 there is no
hook, so we cannot learn the region from `.filter(...)` at all. Two mitigations:

- **Explicit region option**, version-independent: `.option("region", "chr1:1000-2000")`
  (or `referenceName`/`start`/`end` options). This is how a Python user asks for a
  pruned read on 4.0, and it mirrors the Scala API's `GenomicRegion` ergonomics.
- **`pushFilters()` when available** (4.1+): parse `EqualTo` on the reference-name
  column and range comparisons on the coordinate column, exactly like
  `BamScanBuilder`. Return the range filters as *not* fully handled so Spark re-applies
  them post-scan (index queries are overlap queries — same correctness argument as the
  Java version).

```python
# Spark 4.1+; verify the exact return contract against the installed pyspark.
def pushFilters(self, filters):
    unhandled = []
    for f in filters:
        if _is_equal_to(f, self.ref_col):
            self.region_contig = f.value                 # fully handled
        elif _is_range(f, self.coord_col):
            self._absorb_bound(f); unhandled.append(f)   # used for index, re-checked by Spark
        else:
            unhandled.append(f)
    return unhandled
```

No `SupportsPushDownLimit` equivalent exists, so `df.limit(N)` cannot prune planning
the way the JAR does; Spark still applies the limit post-scan (correct, just not I/O
optimal).

### No statistics, no ordering, no column pruning pushdown

The Python API exposes no `SupportsReportStatistics`, `SupportsReportOrdering`, or
`SupportsPushDownRequiredColumns`. Consequences, all correctness-preserving:

- The optimizer gets no `sizeInBytes`/`numRows` hints, so join/broadcast planning is
  blind to genomics file sizes.
- Coordinate-sorted indexed reads cannot advertise their ordering, so
  `ORDER BY chrom, pos` and coordinate merge-joins will insert a sort/shuffle the JAR
  would elide.
- The reader always emits the full schema; Spark prunes columns afterward. We lose the
  BAM optimization where dropping `attributes` skips per-record tag parsing — unless we
  approximate it with an option (e.g. `.option("attributes", "false")`).

### Splitting: how each case is partitioned

Partitioning is where pysam's high-level API and htsjdk diverge. Here is how each case is
handled.

**Indexed, via the high-level API:**

- *Per-contig* — each partition calls `.fetch(contig)`. The default and dominant case;
  htslib seeks via the index to each contig's blocks.
- *Intra-contig by coordinate* — to parallelize one large contig, the driver splits it
  into coordinate windows and each partition calls `.fetch(contig, start_i, end_i)`.
  Boundary-straddling reads are de-duplicated by a start-ownership rule (a read belongs to
  the window whose `[start_i, end_i)` contains its `reference_start`). Each window still
  reads only its own BGZF blocks — the BAI translates the coordinates to virtual file
  offsets and htslib seeks straight to them — so per-partition I/O drops just like a region
  query; only *balance* is approximate (a depth spike makes one window heavier). We accept
  approximate balance rather than build byte-balanced VFO splitting.
- *Region query* — a pushed filter becomes a single `.fetch(contig, start, end)`.

**Unindexed BAM, via a byte-range split with an executor-side guesser** (required — the
JAR parallelizes unindexed BAM and we must match it). pysam has no high-level "resync from
a raw offset," so we build one, porting the well-understood hadoop-bam / htsjdk
`BAMSplitGuesser` approach:

1. *Driver:* divide the file into arithmetic byte chunks of `bgzfSplitSize` (default
   128 MB, matching the JAR), one partition per chunk. No index and no file scan — chunk
   `i` is just `[i·S, (i+1)·S)`.
2. *Executor — find the block:* read raw bytes from the chunk start and scan forward for a
   BGZF block header (`1f 8b 08 04` plus a valid `BC` extra subfield), validating by
   chaining to the next block so a stray `1f 8b 08 04` inside compressed data is rejected.
   Yields a compressed block offset `C`.
3. *Executor — find the record:* decompress from `C` and run a BAM record-boundary guesser
   — try each decompressed offset `u`, interpret it as a record (`block_size`, then
   `refID`/`pos`/`l_read_name`/`n_cigar_op`/`l_seq` plausibility) and accept the first `u`
   whose fields are self-consistent *and* chain to a few more valid records. The record
   start is the virtual offset `V = (C << 16) | u`.
4. *Executor — decode with pysam:* `af.seek(V)` and iterate, stopping when the current
   record's block offset (`af.tell() >> 16`) reaches the next chunk boundary. Decoding,
   CIGAR, tags all stay in pysam, so there is **one** decode path and parity holds.

Ownership is by the compressed offset of the block a record starts in, so a record
straddling a chunk boundary is claimed by exactly one partition (lossless), and this works
on *unsorted* BAMs too — it is byte-based, not coordinate-based. The header (hence the
reference names) is read at open, so mid-file records interpret correctly. SAM (plain
text) splits the same way but trivially — byte ranges with newline resync, skipping `@`
header lines — and the same BGZF finder / `@`-resync extends to `.fastq.gz` and
uncompressed FASTQ.

!!! success "Validated by spike (pysam 0.24.0, against a real BAM)"
    `pysam.AlignmentFile.seek()` **does** accept an externally computed virtual offset on
    an index-less open and reads correctly from it. A pure-Python block finder + record
    guesser was checked exhaustively — from 250,000 byte offsets (249,132 of them
    mid-record) it resynced to the exact next record boundary every time, zero false
    positives — and an end-to-end 8-way chunked split of a 22-block BAM reproduced a full
    sequential read exactly (4,480 records, no gaps or duplicates). The residual task is
    engineering (cap the resync scan at one max BGZF block, ~64 KB), not an open question.

**Not built — by choice:**

- *Byte-balanced single-region splitting.* The JAR divides a region's BAI chunk list into
  equal compressed-byte spans; pysam does not expose that chunk list. We use coordinate
  splitting instead and accept approximate balance (byte-balance is not a requirement).

Unindexed **CRAM** splitting was previously listed here as out of scope; a spike shows it
is feasible by a different route — see the next subsection.

The one genuinely novel piece is the Python BGZF-block finder + BAM record guesser — well-
trodden ground (hadoop-bam, htsjdk `BAMSplitGuesser`), localized to the unindexed BAM path.
Everything else is native pysam or a driver-side coordinate computation.

### Unindexed CRAM splitting (evaluation)

Requested as a follow-up, and a spike shows it is **feasible** — by a different route than
BAM, because CRAM is container-structured (not BGZF) and its containers are
self-describing, so no record guesser is needed. Two approaches, each validated in part
against `range.cram`:

- **Synthesize a CRAI on the driver (recommended, sorted CRAM).** In the spike
  `pysam.index()` built a valid `.crai` from an index-less CRAM **without the reference**
  (indexing reads container/slice headers; it does not decode records). With that CRAI —
  written to scratch and passed via `index_filename` when the source is read-only — an
  unindexed CRAM collapses into the already-planned *indexed* CRAM path. Caveat: CRAI
  building requires a coordinate-sorted CRAM.
- **Enumerate containers in pure Python (general, incl. unsorted).** A ~40-line container
  walker (ITF8/LTF8 + container headers) enumerated every container of `range.cram`, and
  the per-container record counts summed to 112 — matching the file — so the driver can
  obtain exact container byte offsets with no index and no reference, mirroring the JAR's
  container scan. The executor would start at a container boundary; pysam exposes
  `.seek()`/`.tell()` on CRAM and `.tell()` returned the first container's byte offset,
  which makes a seek-to-container hand-off plausible — but seek-then-decode could not be
  confirmed because decoding `range.cram` needs its reference, which is not checked in.
  That single step is the remaining open question.

**Effort and recommendation.** The CRAI-synthesis path is small (a driver-side
`pysam.index()` plus scratch-path handling) and covers the common sorted case, so it is the
pragmatic first step — arguably worth doing since it reuses the indexed path wholesale. The
container-walker path is a modest, well-specified addition that also covers unsorted CRAM
but needs one confirmation spike for the executor-side container seek (run it with a CRAM
whose reference is available). Independently of splitting, every CRAM executor still needs
the reference FASTA on a mounted path — see [CRAM reference resolution](#cram-reference-resolution) — which is unchanged.

### The `_metadata` column

The JAR exposes a hidden, Databricks-compatible `_metadata` struct on every format. The
Python Data Source API has no mechanism to contribute hidden metadata columns. Phase 1
omits it. If needed, we can expose an **opt-in real column** (`.option("metadata",
"true")` adding a `_metadata` struct to the visible schema), accepting that it changes
the schema and therefore breaks strict parity when enabled.

### CRAM reference resolution

CRAM needs a reference to decode sequence. pysam takes `reference_filename=` (a
filesystem path to the FASTA) and honors `$REF_PATH`/`$REF_CACHE` for htslib's MD5-based
reference lookup. We map the JAR's `referenceFile`/`referenceMode` options onto these;
the reference FASTA (and its `.fai`) must be on a mounted path, same as the CRAM itself.

## Coexistence and naming

We assume a deployment installs **either the JAR or the Python package, not both** — they
are two implementations of the same reader for two different deployment models. The Python
source therefore registers under the **same short names** (`bam`, `cram`, …) with no
prefixing or precedence to worry about; `spark.read.format("bam")` resolves to whichever
implementation is installed. Parity between the two is enforced offline, by comparing the
Python reader against checked-in golden output generated from the JAR (see
[Testing](#testing-strategy)) — so the test suite never needs both loaded at once either.

## Phased implementation plan

- **Phase 0 — scaffolding & parity harness.** `pyproject.toml`, `register_all`,
  `schemas.py` with golden schema-JSON parity tests, Arrow batch builders, and the
  filesystem-path I/O layer (path normalization: `dbfs:/`→`/dbfs/`, pass through
  `/Volumes` and local paths, reject raw cloud URIs). Land **FASTA**, **FASTQ**, **BED**
  first — the simplest formats (FAI whole-contig, streaming Fastx, tabix/line BED) — to
  exercise the whole pipeline end to end.
- **Phase 1 — indexed formats.** **BAM/SAM/CRAM** and **VCF/BCF**: per-contig partition
  planning, region reads via `.fetch()`, the explicit `region` option, and `pushFilters()`
  gated on Spark 4.1+. Correctness verified on local and mounted paths.
- **Phase 2 — unindexed BAM splitting (required).** The executor-side BGZF-block finder +
  BAM record-boundary guesser, handing off to `pysam.seek(voffset)` (see the Splitting
  section); SAM line-range splits; the same machinery extended to `.fastq.gz` and
  uncompressed FASTQ. Spike first to confirm `.seek()` accepts an externally computed
  virtual offset on an index-less open.
- **Phase 3 — FUSE random-access validation (superseded for S3/GCS/Azure, still open for
  everything else).** S3/GCS range-request behavior and Azure's download-then-open path are
  now verified directly (native reads through the real Spark pipeline against MinIO/
  fake-gcs-server/Azurite — see `python/tests/test_cloud_{s3,gcs,azure}.py`), not via a FUSE
  mount at all. What's left of this item: benchmark an indexed region read over a UC Volume
  or a self-managed `mountpoint-s3`/`s3fs` mount for the schemes that still require one
  (`adl://`, `hdfs://`) — the Databricks analog of the `*RangeS3Test` suite. If a mount
  streams whole files, document it and add an opt-in localize-then-read fallback for large
  indexed reads (the same pattern Azure's `_cloudfs.materialize_local` already implements).
- **Phase 4 (optional) — intra-contig coordinate splitting.** Coordinate sub-range splits
  with start-ownership de-duplication, for skew on files with few contigs (see the
  Splitting section).

Phases 0–2 are the required core (indexed reads plus unindexed BAM parallelism); Phase 3
is a validation gate on the I/O-reduction claim; Phase 4 is an enhancement.

## Testing strategy

- **Docker, like the rest of the repo.** A new `python-test` compose service on a
  `python:3.12` + `pyspark` + `pysam` image, mirroring the Java Docker discipline.
- **Golden parity against the JAR.** Generate a canonical dump (deterministically sorted
  rows + schema JSON) from the JAR once per fixture, check it in as a golden file, and
  assert the Python reader reproduces it exactly for every checked-in fixture
  (`range.bam`, `range.cram`, `realn01.fa`, the FASTQ files, `example.bed.gz`, generated
  VCFs). Reuse the existing `core/src/test/resources` fixtures directly — no new test
  data. Comparing against goldens (rather than running both readers in one session) keeps
  the Python suite JAR-free, consistent with the one-implementation-per-deployment
  assumption.
- **Schema parity.** Assert Python `StructType.json()` equals the JAR schema JSON
  (checked-in golden), for every format and for `columnNames=sam`.
- **Region-read / range-request verification — direct, not over FUSE.** `test_cloud_s3.py`
  and `test_cloud_gcs.py` read directly against MinIO/fake-gcs-server (`s3://`/`gs://`, no
  mount) through the real Spark pipeline and assert region-query correctness against a local
  pysam oracle; `test_cloud_azure.py` does the same for `abfss://` against Azurite via the
  download-then-open path. Byte-transfer-reduction assertions (matching the JAR's
  `*RangeS3Test`) are a known gap for the Python suite — no Hadoop-`FileSystem`-statistics
  equivalent exists to measure it against; see `TASKS.md`. The FUSE-mount version of this
  item (Phase 3, below) is still open, but now only for the schemes that still need a mount.
- **Coordinate-convention tests.** Explicit assertions on 1-based `start` vs. 0-based
  `start0` (BAM) and 0-based half-open `chromStart`/`chromEnd` (BED), since the pysam
  offset conventions differ from htsjdk's and this is the likeliest source of silent
  drift.

## Packaging and distribution

- **PyPI package `lite-bfx-spark`**, importable as `litebfx`. Runtime deps:
  `pysam>=0.22`, `pyarrow`, `pyspark>=4.0`. pysam ships manylinux + macOS wheels, so no
  from-source htslib build is needed (a real simplification over the JAR's samtools
  image).
- **Publish workflow** analogous to `publish.yml`: build the wheel and sdist, publish
  to PyPI on a tag. Keep versioning in step with the JAR so `1.2.0` of each is the same
  feature set.
- **Docs:** a new `docs/python-api.md` reference page (registration, options, the
  supported-schemes matrix, the explicit gaps table) and a Python tab alongside the
  Scala examples on the format pages.

## Risks and open questions

1. **Unindexed-split resync — spike done, PASSED.** The load-bearing questions for the
   required unindexed BAM path are resolved: against a real BAM (pysam 0.24.0),
   `AlignmentFile.seek()` accepts an externally computed virtual offset on an index-less
   open, the pure-Python record guesser resynced correctly from all 250,000 tested offsets
   (249,132 mid-record, zero false positives), and an 8-way chunked split reproduced a full
   sequential read exactly. Remaining work is engineering only: bound the guesser's scan to
   one max BGZF block (~64 KB) and port it into the reader.
2. **FUSE random-access performance (scoped down).** No longer applies to S3/GCS (native
   htslib reads, range-request behavior verified directly against MinIO/fake-gcs-server) or
   Azure (download-then-open — one full transfer, then local-speed reads, a different and
   already-understood performance story). Still open for `adl://`/`hdfs://`-style access via
   a UC Volume or self-managed mount: does the mount translate pysam's seek-heavy `.fetch()`
   into range reads, or stream/cache whole files? **Benchmark before claiming I/O reduction
   for those remaining schemes (Phase 3).**
3. **`pushFilters` contract by version.** The exact `pushFilters` signature and return
   semantics are Spark 4.1+; confirm against the pinned pyspark and gate cleanly so Spark
   4.0 falls back to the explicit `region` option.
4. **Intra-contig parallelism vs. skew.** Coordinate sub-range splitting (Phase 4) is
   depth-skewed, not byte-balanced like the JAR's VFO chunks. On a single huge file whose
   reads concentrate in a few coordinate windows, partitions will be uneven; byte-balanced
   splitting would need low-level htslib chunk access we have chosen not to build.
5. **Throughput.** Even with Arrow batches, a Python/pysam reader will likely trail the
   shaded-htsjdk JAR on large scans. Benchmark early and set expectations rather than
   imply parity on speed.

## Recommendation

Proceed with Phases 0–2 as the required core: all six formats with filesystem-path I/O,
Arrow output, and the schema/parity harness (0); indexed region and per-contig reads (1);
and unindexed BAM splitting via the block + record guesser (2). Phase 3 (the FUSE
random-access benchmark) now only gates the I/O-reduction claim for schemes without a
native-or-download story (`adl://`, `hdfs://`) — S3/GCS (native htslib reads) and Azure
(download-then-open) shipped with their own direct verification instead of a FUSE mount; add
a localize-then-read fallback only if a *remaining* mount-only scheme proves to stream whole
files (Azure's `_cloudfs.materialize_local` is already exactly that fallback, applied
unconditionally rather than as a probe result). Phase 4 (intra-contig coordinate splitting)
is an enhancement driven by real skew. Position the Python package as the **JAR-free,
pip-installable** reader — S3, GCS, and Azure direct, FUSE-mounted paths for everything
else — complementary to the JAR, not a replacement for its optimizer integrations,
byte-balanced region splitting, or Hadoop-native access to the schemes it still doesn't
cover directly.
