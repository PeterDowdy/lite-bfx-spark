# `litebfx` (Python module) — implementation task list

Durable, resumable plan for the pure-Python Spark Data Source described in
[docs/proposals/python-data-source.md](../docs/proposals/python-data-source.md).
Update the checkboxes as work lands. `[~]` = in progress.

## Goal

Ship `pip install lite-bfx-spark` (import `litebfx`) implementing Spark 4.0 Python Data
Source readers for BAM/SAM/CRAM, VCF/BCF, FASTA, FASTQ, BED — schema-identical to the
Java JAR, filesystem-path I/O only (FUSE mounts for cloud), Arrow output.

## Validated decisions (do not re-litigate)

- **Parsing:** pysam 0.24.0 (bundled htslib). One decode path.
- **Cloud I/O:** local/FUSE paths only; normalize `dbfs:/`→`/dbfs/`, pass `/Volumes` &
  local through, **reject** raw `s3://`/`abfss://`/`gs://`. No SDK/credentials in-package.
- **Intra-contig parallelism:** coordinate sub-ranges + start-ownership dedup
  (approximate balance accepted; byte-balanced VFO splitting NOT built).
- **Unindexed BAM split:** SPIKE PASSED. `pysam.seek(voffset)` works index-less;
  pure-Python BGZF finder + record guesser proven (250k offsets, 0 errors); 8-way split
  lossless. Port from `spikes/bam_spike.py` into `src/litebfx/bgzf.py`, add a bounded
  (~64 KB) scan window.
- **Unindexed CRAM split:** feasible (evaluated). Preferred: driver-side `pysam.index()`
  to synthesize a CRAI (sorted CRAM; builds without a reference) → reuse indexed path.
  General/unsorted: pure-Python container walk (`spikes/cram_probe.py`) — needs one more
  spike for executor-side container seek (blocked locally by missing reference).
- **Pushdown:** `pushFilters()` gated to Spark 4.1+; explicit `.option("region", ...)`
  works on 4.0. No limit/column/stats/ordering pushdown (API has no hooks).
- **Coexistence:** assume JAR **or** package, not both → same short names, no aliasing.
- **BED numbers:** no truncation — samtools never truncates a BED numeric column, so a
  decimal numeric column → null (narrowPeak `19.76368` → null, not `19`). **The JAR was
  updated to match** (`BedPartitionReader`, previously truncated). Shared conventions:
  `.`/`0` absent-markers → null, packed-int itemRgb → `R,G,B`, block trailing-comma stripped,
  malformed interval dropped. Verified against `samtools view`: Python
  `tests/test_samtools_parity.py`, Java `BamSamtoolsParityTest`.

## CI/CD design (mirror the Java library)

| Java | Python analog |
|---|---|
| `ci.yml` staging-publish on push→main, version `0.0.0-main-<sha>` → Sonatype staging | push→main → **TestPyPI**, version `0.0.0.dev<run_number>` (PEP 440, monotonic) |
| `publish.yml` on tag `v*.*.*` → Maven Central + GitHub Release | tag `v*.*.*` → **PyPI**, version = tag minus `v` + GitHub Release |

- Publishing via PyPI **Trusted Publishing (OIDC)** — no API tokens/secrets. Each publish job
  has `permissions: id-token: write`; configure a trusted publisher on PyPI and TestPyPI for
  this repo (workflows `python-publish.yml` / `python-ci.yml`).
- Test matrix: Python 3.10–3.12 (pysam wheels), Spark 4.0.x, JDK 17 in CI (local box is JDK 25 — Spark 4 may not start there).

## Module layout

```
python/
  pyproject.toml            # package lite-bfx-spark; deps pysam, pyarrow, pyspark>=4.0
  src/litebfx/
    __init__.py             # register_all(spark), __version__
    schemas.py              # StructTypes == Java *Schema (incl. columnNames=sam)
    io.py                   # path normalization; reject raw cloud URIs
    regions.py              # region option + pushFilters parsing
    arrow.py                # StructType -> pyarrow schema; RecordBatch builders
    bgzf.py                 # BGZF block finder + BAM record guesser (from spike)
    fasta.py fastq.py bed.py vcf.py bam.py cram.py
  spikes/                   # validation scripts (done)
  tests/
    conftest.py             # local SparkSession
    golden/                 # checked-in JAR-parity goldens (schema JSON + row dumps)
    test_*.py
```

## Tasks

**Status: implemented; full suite green in Docker (`docker compose run --rm python-test`,
JDK 17 + PySpark 4.0, 29 passing).**

- [x] Save spikes to `python/spikes/` (+ README)
- [x] Write this task list
- [x] `pyproject.toml` + package skeleton + `__init__.register_all`
- [x] `schemas.py` — 6 formats, parity with Java `*Schema` (BAM `descriptive`+`sam`)
- [x] `io.py` (path normalize/reject), `regions.py` (region option + duck-typed pushFilters)
- [x] `bgzf.py` — block finder + record guesser, bounded window (validated lossless)
- [x] Readers: `fasta.py`, `fastq.py`, `bed.py`
- [x] Readers: `vcf.py`, `bam.py` (indexed per-contig + region + unindexed split), `cram.py`
- [x] Tests: **parity** for schema (independent Java triples) + record values (independent
      pysam/`to_string` oracles; VCF against hand-written expected values)
- [x] Tests: unindexed-BAM split — **losslessness only, NOT parity** (union == full read,
      count, no dups; multi-partition)
- [x] Tests: pure-Python units (bgzf/guesser lossless, io, regions, bed parse, read_number)
- [x] `docker/Dockerfile.python` + compose `python-test` service (JDK 17)
- [x] `.github/workflows/python-ci.yml` — ruff + pytest matrix (py3.10–3.12) + TestPyPI staging on main
- [x] `.github/workflows/python-publish.yml` — PyPI release on tag
- [x] Fix README discrepancy (FASTQ `readNumber` was missing) + add pip pointer

### Implemented since first cut

- Predicate **pushdown** (`pushFilters`, Spark 4.1+) — BAM/CRAM, VCF, BED (index-guarded; the
  explicit `region` option works on Spark 4.0).
- **Multi-file / glob / directory reads** and `indexPath`/`indexDir` options (all formats).
- **_metadata** opt-in column (`.option("metadata","true")`) — a visible struct (`file_path`,
  `file_name`, `file_size`, `file_modification_time`, `index_path`) on every format.
- **FASTQ** uncompressed byte-range splitting (each split resyncs to the next 4-line record).
- **Unindexed CRAM** splitting — the driver synthesizes a co-located CRAI (`pysam.index`) for a
  coordinate-sorted CRAM in a writable directory, then reads per-contig; single partition otherwise.
- **Arrow output** — all readers yield `pyarrow.RecordBatch` (the columnar fast path).
- `DIRECTORY.md` regenerated to index `python/`.

### Still deferred (documented limitations)

- BGZF `.fastq.gz` splitting — plain-gzip `.fastq.gz` is a single partition (the common case).
- Unindexed CRAM in a **read-only** directory falls back to a single partition (no shared CRAI).
- `_metadata` is a **visible** opt-in column, not the JAR's hidden `_metadata` (the Python
  DataSource API cannot contribute hidden columns).

## Parity-testing note (per request)

Golden files are generated once from the JAR and checked in; the Python suite compares
against them (stays JAR-free). **Everything gets a parity test except the coordinate-vs-
byte-chunk unindexed BAM split**, which is validated for *losslessness* (union of
partitions == full sequential read, exact count, no duplicates) but **not** for
partition-level parity — the Python coordinate/byte split intentionally produces different
partition boundaries than the JAR.

## How to run (dev)

```bash
cd python
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[test]'
pytest -q                       # unit + parity (needs JDK 17 for the Spark-backed tests)
pytest -q -m "not spark"        # pure-Python units only (no JVM)
```

## Open questions / follow-ups

- Executor-side CRAM container seek (`.seek()` + decode) — spike with a reference-present CRAM.
- Confirm exact `pushFilters` return contract on the pinned Spark 4.1.x.
- FUSE random-access benchmark on UC Volumes / `mountpoint-s3` (proposal Phase 3).
