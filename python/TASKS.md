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
- **Cloud I/O (superseded, see below):** `s3://`/`s3a://`/`s3n://`/`gs://`/`gcs://` are now
  opened **directly** — pysam's bundled htslib has a native S3/GCS remote-read backend
  (confirmed by binary inspection + live MinIO/fake-gcs reads through the real Spark
  pipeline), no FUSE mount needed. `abfss://`/`wasbs://`/`abfs://`/`wasb://` are also opened
  directly, but via a different mechanism: htslib has **no native Azure backend at all**
  (confirmed absent), so the object is downloaded to a local temp file via
  `pyarrow.fs.AzureFileSystem` and pysam opens the local copy — no range-request efficiency
  for Azure specifically, but no FUSE mount either. `adl://`/`hdfs://`/`http(s)://`/`ftp://`
  are still rejected, unchanged, with FUSE-mount guidance. Credential handling: ambient
  (env vars / instance metadata / shared config, resolved by htslib/pyarrow.fs themselves)
  everywhere, **except GCS off Databricks**, where the `gcp` extra mints htslib's required
  `GCS_OAUTH_TOKEN` from an ambient `GOOGLE_APPLICATION_CREDENTIALS` key file (htslib never
  reads that env var itself — see `_cloud.py`'s module docstring); on Databricks, `_cloud.py`
  additionally vends short-lived, path-scoped credentials via Unity Catalog's Temporary
  Credentials API and prefers them over both ambient resolution and the `gcp`-extra mint.
  See `io.py`, `_cloudfs.py`, `_cloud.py`, and `docs/proposals/python-data-source.md`'s
  "Cloud I/O" section for the full design.
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
- **Pushdown:** `pushFilters()` gated to Spark 4.1+ **and** `spark.sql.python.filterPushdown
  .enabled=true` (defaults false on every 4.1+ build — Spark errors at plan time if
  pushFilters() is implemented while the conf is off). `register_all()` checks the conf
  once, driver-side, and registers a reader class with or without `pushFilters()` to match
  — a `DataSourceReader` can't see this conf itself (worker process, no SparkSession); see
  `__init__.py`. Explicit `.option("region", ...)` works everywhere, conf or no conf.
  No limit/column/stats/ordering pushdown (API has no hooks).
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
    io.py                   # path normalization; scheme gate (native / download / rejected)
    _cloudfs.py             # pyarrow.fs dispatch: local-vs-cloud exists/stat/listdir/reads
    _cloud.py               # cloud credentials: ambient + Databricks UC vending
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
- **Ambient GCS credential minting off Databricks** (`gcp` extra) — `_cloud.py`'s
  `prepare_env()` mints htslib's required `GCS_OAUTH_TOKEN` from an ambient
  `GOOGLE_APPLICATION_CREDENTIALS` service-account key file, cached and refreshed near
  expiry, whenever Databricks UC vending didn't already supply one. Closes a real gap: GCS
  was previously the one cloud where "ambient credentials just work" wasn't true, since
  htslib never reads `GOOGLE_APPLICATION_CREDENTIALS` itself. See `_mint_ambient_gcs_token()`
  and `python/tests/test_cloud_gcp_ambient.py`.
- **Fixed: stale `_cloudfs.py` filesystem cache poisoning UC-vended reads** — reported as a
  clean, consistent `AWS ACCESS_DENIED`/403 on a Databricks Serverless S3 path the reporting
  user could otherwise access. Root cause: `filesystem_for()` cached the constructed
  `pyarrow.fs.S3FileSystem` by `(scheme, bucket)` *before* ever consulting
  `_cloud.credentials_for()`, so a single transient vending failure on the first cloud touch
  in a worker process (network blip, `WorkspaceClient()` not yet warmed up, etc.) permanently
  pinned that bucket to the unauthorized ambient-fallback filesystem for the rest of the
  process's life — even though `_cloud.credentials_for()` itself already retries vending on
  every call while its own cache holds `None`. The symptom looked exactly like a permissions
  problem rather than the one-time-hiccup-then-cache-poisoning it actually was. Fixed by
  folding credential identity into the cache key (`filesystem_for()`'s docstring has the full
  writeup) so a refreshed or retried credential naturally invalidates the stale entry, with
  `_cache_fs()` evicting the old entry so the cache doesn't grow unbounded across a
  long-running process's credential refreshes. `vend_credential()`'s failures (exception,
  databricks-sdk not importable, unrecognized response shape) are now also logged instead of
  silently swallowed — the fallback-to-ambient behavior itself is still deliberate and
  correct, but a fallback that then *also* fails previously gave zero indication vending was
  even attempted. See `python/tests/test_cloudfs_cache.py` and the new logging tests in
  `test_cloud_databricks.py`. **Not yet confirmed against the reporting user's live
  workspace** — two separate install-time blockers (see the next two entries) prevented a
  clean retest; genuinely unknown yet whether this fix alone resolves the original report.
- **Fixed: `pyarrow` as a hard dependency broke installs on Databricks dedicated compute** —
  surfaced while trying to get a clean install of the fix above onto a real workspace for
  verification. `pyproject.toml` declared `pyarrow>=4` unconditionally, but every Databricks
  Runtime image (classic or serverless) already bundles its own runtime-pinned `pyarrow`
  tightly coupled to internal Spark/Arrow machinery — `pip install` on dedicated compute
  (version-pinned images, unlike a from-scratch environment) tries to reconcile against that
  pin and conflicts. `pyspark` already got this right (an extra, not a hard dependency,
  specifically because it's runtime-provided); `pyarrow` now follows the same rule, moved
  into the `spark`/`test` extras only. This also fixes a pre-existing docs/code mismatch —
  `python/README.md` already (incorrectly) claimed "pysam is the only hard dependency" while
  `pyarrow` was also unconditionally required. See `python/README.md`'s new "Installing on
  Databricks dedicated compute" section for the `--no-deps` recipe as a further fallback
  (e.g. for a runtime-pinned `databricks-sdk` conflict, which loosening `pyarrow` alone
  doesn't address).
- **Fixed: exactly that predicted `databricks-sdk` conflict, for real** — the very next
  retest hit a setuptools incompatibility installing `lite-bfx-spark[databricks]` on
  dedicated compute. `databricks-sdk` doesn't declare `setuptools` as a runtime dependency
  itself (confirmed against PyPI metadata), but it's preinstalled on every Databricks
  Runtime/Serverless image regardless (Databricks' own tooling depends on it) — so the
  `databricks` extra never needed to declare it at all, for the same reason `pyarrow`
  shouldn't have: doing so just forces pip to reconcile against whatever version and
  transitive chain is already pinned there. Removed the `databricks` extra entirely (moved
  `databricks-sdk` into `test` only, since this repo's own Docker test images don't have it
  preinstalled the way real Databricks compute does); `_cloud.py`'s
  `_import_databricks_sdk()` already degraded gracefully when it's absent, so nothing in the
  runtime behavior changes off the extras mechanism itself — only the install-time story
  does. Off Databricks, install it directly (`pip install databricks-sdk`) if exercising the
  vending code path without a real workspace.

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
- FUSE random-access benchmark on UC Volumes / `mountpoint-s3` — now scoped to the schemes
  that still need a mount (`adl://`, `hdfs://`), not S3/GCS/Azure, which no longer depend on
  FUSE at all.
- **htslib upstream contribution opportunity**: htslib's S3 backend (`hfile_s3.c`) supports a
  custom endpoint via `HTS_S3_HOST`/`HTS_S3_ADDRESS_STYLE`, but the GCS backend
  (`hfile_gcs.c`) has no equivalent at all — confirmed by binary inspection (only
  `GCS_OAUTH_TOKEN`/`GCS_REQUESTER_PAYS_PROJECT` are read; the URL rewrite is hardcoded to
  `.googleapis.com`) and by direct testing (a `gs://` read against fake-gcs-server hangs
  30+ minutes retrying real Google OAuth2 endpoints before failing). Adding a GCS host
  override mirroring the existing S3 one would let htslib-based tools (this package
  included) test against `fake-gcs-server`/`gcs-emulator`, not just real GCS buckets — a
  genuinely useful, scoped upstream fix for github.com/samtools/htslib, not just a litebfx
  workaround. `test_cloud_gcs.py` documents the current workaround (orchestration-layer-only
  testing via `pyarrow.fs.GcsFileSystem`'s explicit `endpoint_override`).
- Databricks UC credential vending (`_cloud.py`): whether `WorkspaceClient()`'s default auth
  resolves inside the isolated Python Data Source worker subprocess (vs. only the driver) is
  unverified without a real workspace — see `tests/smoke_uc_credential_vending.py`, run
  manually pre-release, not in default CI. A real Serverless 403 report traced to a *different*
  bug (stale `_cloudfs.py` filesystem cache, now fixed — see "Implemented since first cut"),
  not this one, so this question is still open, not resolved by that fix.
- Whether `_cloud.is_databricks()`'s single-signal check (`DATABRICKS_RUNTIME_VERSION` env
  var) reliably detects Databricks **Serverless** compute specifically (vs. classic clusters,
  where it's long-established to work) is unconfirmed — flagged during the Serverless 403
  investigation above but not yet verified either way. If it turns out to under-detect
  Serverless, `vend_credential()` would now at least log *why* (`is_databricks()` returning
  False makes it return early with no log line at all, by design, since that's also the
  correct behavior for the common "genuinely not on Databricks" case) — so confirm via the
  now-logged INFO/WARNING output first before assuming this is the cause.
- `_cloud.py`'s cloud-read lock serializes all cloud-backed reads within one worker process
  (a deliberate correctness-over-parallelism tradeoff — see its module docstring) because
  it's unverified whether htslib re-reads env vars only at open time or per range-request.
  Revisit only if this is a measured bottleneck.
- Azure download-fallback (`_cloudfs.materialize_local`) caches by source URL for a worker
  process's lifetime but doesn't bound total local disk usage across distinct files — a
  long-running worker touching many large Azure files could accumulate significant local
  temp storage. Acceptable for typical partition-plan sizes; revisit if it becomes an issue.
