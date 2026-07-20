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
- **Cloud I/O:** `s3://`/`s3a://`/`s3n://`/`gs://`/`gcs://` are opened **directly** — pysam's
  bundled htslib has a native S3/GCS remote-read backend (confirmed by binary inspection +
  live MinIO/fake-gcs reads through the real Spark pipeline), no FUSE mount needed.
  `abfss://`/`wasbs://`/`abfs://`/`wasb://` are also opened directly, but via a different
  mechanism: htslib has **no native Azure backend at all** (confirmed absent), so the object
  is downloaded to a local temp file via `pyarrow.fs.AzureFileSystem` and pysam opens the
  local copy — no range-request efficiency for Azure specifically, but no FUSE mount either.
  `adl://`/`hdfs://`/`http(s)://`/`ftp://` are still rejected, unchanged, with FUSE-mount
  guidance. Credential handling: ambient (env vars / instance metadata / shared config,
  resolved by htslib/pyarrow.fs themselves) everywhere off Databricks. **Except GCS**, where
  the `gcp` extra mints htslib's required `GCS_OAUTH_TOKEN` from an ambient
  `GOOGLE_APPLICATION_CREDENTIALS` key file (htslib never reads that env var itself — see
  `_cloud.py`'s module docstring) — this applies the same way everywhere, Databricks or not.
  **On Databricks, S3 paths get a driver-vended Unity Catalog path credential instead of
  ambient resolution** (AWS only for now) — the second, structurally different attempt at
  this goal, after a first "worker calls `WorkspaceClient()` directly" attempt was built,
  diagnosed against a real workspace, and removed (see "Implemented since first cut" for both
  the removal and the replacement, and why the replacement sidesteps the exact problem that
  sank the first one). See `io.py`, `_cloudfs.py`, `_cloud.py`, and
  `docs/proposals/python-data-source.md`'s "Cloud I/O" section for the full design.
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
- **Installing a TestPyPI staging build needs `--extra-index-url`.** TestPyPI is not a mirror
  of real PyPI — it only has what this project itself has published there. A bare
  `pip install --index-url https://test.pypi.org/simple/ lite-bfx-spark` confines pip to
  TestPyPI for *every* dependency, including third-party ones like `pysam` that were never
  published there — pip falls back to whatever stray old artifact happens to exist (an sdist,
  forcing a source build), whose own build-time `setuptools` requirement then *also* isn't
  resolvable there, surfacing as a confusing `setuptools>=X ... from versions: none` error
  that has nothing to do with litebfx's own dependency declarations. Fix: add
  `--extra-index-url https://pypi.org/simple/` so pip falls through to real PyPI for anything
  TestPyPI doesn't have:
  ```bash
  pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ lite-bfx-spark
  ```
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
    _cloud.py               # cloud credentials: ambient + GCS minting + Databricks AWS vending
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
- **Ambient GCS credential minting** (`gcp` extra) — `_cloud.py`'s `prepare_env()` mints
  htslib's required `GCS_OAUTH_TOKEN` from an ambient `GOOGLE_APPLICATION_CREDENTIALS`
  service-account key file, cached and refreshed near expiry. Closes a real gap: GCS was
  previously the one cloud where "ambient credentials just work" wasn't true, since htslib
  never reads `GOOGLE_APPLICATION_CREDENTIALS` itself. See `_mint_ambient_gcs_token()` and
  `python/tests/test_cloud_gcp_ambient.py`.
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
  `test_cloud_databricks.py`. **Confirmed via a worker-side diagnostic against the reporting
  user's live workspace**: this fix was necessary but not sufficient on its own — two
  install-time blockers (next two entries) had to clear first before a clean retest was even
  possible, and once they did, a captured `litebfx._cloud` log line from inside a real
  distributed task pinpointed the actual remaining cause precisely (see the SDK-version-gap
  entry below), not a further cache issue. So: real bug, real fix, but not the *whole* story
  for this specific report.
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
- **Fixed: the actual remaining cause — `generate_temporary_path_credentials` doesn't exist
  in the databricks-sdk bundled with Databricks Runtime 17.3 LTS at all.** Found via a
  worker-side diagnostic against the reporting user's live workspace that captured
  `vend_credential()`'s own log output directly (rather than relying on wherever Databricks
  surfaces isolated-worker-subprocess stderr, which is exactly the kind of thing that's hard
  to track down remotely): `ImportError: cannot import name 'PathOperation' from
  'databricks.sdk.service.catalog'`. Confirmed empirically (downloaded and inspected the
  actual wheels from PyPI, and looked up each Databricks Runtime's own "Installed Python
  libraries" release-notes table rather than guessing) that this is not a fluke:

  | Databricks Runtime | Spark | bundled databricks-sdk | path-scoped vending? |
  |---|---|---|---|
  | 16.4 LTS | 3.5.2 | 0.30.0 | n/a — Spark 3.x can't run litebfx's Python Data Source at all |
  | 17.3 LTS | 4.0.2 | 0.49.0 | No — only a *table*-scoped `generate_temporary_table_credentials(table_id=...)`, unusable here since litebfx reads arbitrary cloud paths, not paths pre-resolved to a registered UC table |
  | 18.0 | 4.1.0 | 0.67.0 | Yes |
  | Serverless (classic-DBR-independent versioning, e.g. `DATABRICKS_RUNTIME_VERSION="client.5.8"`) | — | client ≥5 bundles 0.67.0 | Yes from client generation 5 |

  A different, older API (`CredentialsAPI.generate_temporary_service_credential`, present in
  0.49.0, response-shape-compatible with `_credential_from_response()` with no new parsing
  code needed) was investigated as a possible 17.x fallback — technically reachable via
  `external_locations.list()` + client-side URL-prefix matching to find the right
  `credential_name` — but its own docstring requires "metastore admin or the metastore
  privilege **ACCESS** on the service credential," a materially different and typically
  *more* privileged grant than the `READ FILES` on an External Location that path-scoped
  vending needs; most UC setups reserve direct storage-credential `ACCESS` for admins.
  Building this risked trading one confusing permission error for a different one, for an
  outcome not confirmed to actually work for the reporting user's identity — decided against
  it (user's call, given the added complexity and uncertain payoff) in favor of:

  1. `_import_path_credentials_api()` — an actual-import-based check (the source of truth)
     that makes `vend_credential()` fail with one clear, specific log line ("this SDK version
     doesn't support path-scoped vending, expected on 17.x-and-earlier") instead of letting a
     raw `ImportError` traceback escape into the generic exception handler and look like a
     bug or a permissions problem.
  2. `uc_path_vending_warning()` + `_databricks_runtime_info()` — a *proactive*,
     version-string-based heuristic (parses `DATABRICKS_RUNTIME_VERSION`, handling both
     classic DBR's plain numeric scheme and Serverless's separate `client.N.M` scheme) that
     `register_all()` checks once at setup time, so a user on an unsupported runtime finds
     out immediately via a `UserWarning` in their notebook — not lazily, buried in a worker
     log, on first failed cloud read. Deliberately kept as a *second*, complementary check
     rather than replacing (1): this one infers from a version string and could in principle
     be wrong (a manually-upgraded SDK on an old runtime, or a future Databricks bundling
     policy change); (1) checks the real installed SDK and stays correct regardless.

  Net effect for Runtime 17.x and earlier: direct `s3://`/`gs://`/`abfss://` reads fall back
  to ambient credential resolution, same as off Databricks entirely. This still works on a
  classic cluster with an instance profile configured (that predates UC vending and is
  independent of it) but not on Serverless below client generation 5, which has no
  instance-profile equivalent — a real, currently-unclosed gap there, not just a rough edge.
- **Removed: the entire Unity Catalog credential-vending feature, after the fix above
  finally exposed the actual structural blocker underneath.** The reporting user's workspace
  reached Runtime 18.2 (bundled SDK has `generate_temporary_path_credentials`), and the very
  same worker-side log-capturing diagnostic that found the SDK-version gap above caught the
  real, final cause: `vend_credential()`'s `WorkspaceClient()` construction failing with

  ```
  ModuleNotFoundError: No module named 'dbruntime'
  ```

  `dbruntime` is a Databricks-internal package available in notebook/driver contexts but not
  in the isolated Python Data Source worker's stripped-down venv
  (`/local_disk0/.ephemeral_nfs/envs/pythonEnv-.../`) — `databricks-sdk`'s default auth chain
  tries a "runtime native auth" strategy first that depends on it, and that failure cascades
  into the whole chain failing. A follow-up diagnostic (dumping `DATABRICKS_HOST`/`TOKEN`/
  `CLIENT_ID`/`CLIENT_SECRET`/`ACCOUNT_ID` presence from inside the same worker context, per
  Databricks' own [unified authentication
  docs](https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication)
  — never values, these would be real secrets) confirmed the worker has **no ambient
  Databricks credential material of any kind**. This is the question `_import_path_
  credentials_api()`'s docstring and `tests/smoke_uc_credential_vending.py` had flagged as
  open since the feature's first implementation: whether `WorkspaceClient()`'s default auth
  resolves inside the isolated worker subprocess. Now answered, decisively: **no, not with
  today's databricks-sdk auth model** — not a litebfx bug, a structural gap between how
  Python Data Source workers are isolated and what the SDK's zero-config auth expects.

  Automatic, zero-config UC vending from inside a worker isn't achievable without the user
  explicitly provisioning and threading through their own credentials (e.g. a service
  principal's `DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET` injected into the compute
  environment) — a materially different, much larger feature than "vending just works," and
  not one litebfx can make transparent the way the rest of this investigation aimed for. User
  decision: remove the feature entirely rather than ship something requiring bespoke setup
  per workspace. Removed: `vend_credential()`, `credentials_for()`,
  `_credential_from_response()`, `_VendedCredential`-for-Databricks, `is_databricks()`,
  `_databricks_runtime_info()`, `uc_path_vending_warning()`, `_import_databricks_sdk()`,
  `_import_path_credentials_api()`, the credential-identity-aware cache in `_cloudfs.py`
  (reverted to plain `(scheme, bucket)` caching), `register_all()`'s setup-time warning,
  `test_cloud_databricks.py`, `test_cloudfs_cache.py`, `tests/smoke_uc_credential_vending.py`,
  and the `databricks-connect-uc` compose service. `_VendedCredential` itself and the GCS
  ambient-minting bullet above are unaffected — that ambient-token-minting design pattern
  outlived the Databricks-specific feature it was originally paired with.
- **Re-implemented: Unity Catalog credential vending, take two — vend once on the driver,
  thread plain credential values to workers.** The removal above answered "does
  `WorkspaceClient()`'s auth resolve inside an isolated worker subprocess?" with a hard no —
  but that only rules out *workers self-authenticating to Databricks*, not vending
  altogether. Databricks support, engaged after the removal, confirmed a different shape
  works for the reporting workspace: fetch short-lived AWS STS credentials **once, on the
  driver**, via a raw REST call to Unity Catalog's Temporary Path Credentials API
  (`POST /api/2.1/unity-catalog/temporary-path-credentials`), authenticated through the
  notebook context's own ephemeral API token (`dbutils` — not `databricks-sdk`, which this
  project still doesn't depend on; see `pyproject.toml`'s comment) — then thread the
  resulting plain credential *values* (`access_key_id`/`secret_access_key`/`session_token`,
  not a live SDK client) to workers via the one channel Python Data Source actually
  guarantees crosses the driver→worker boundary: `InputPartition`. Workers just set three env
  vars; they never authenticate to Databricks themselves, so the `dbruntime`/isolated-venv
  problem that killed the first attempt never comes up — there's nothing worker-side left to
  authenticate. Prerequisite: the identity needs `EXTERNAL USE LOCATION` on the external
  location backing the path — a different, narrower grant than the `READ FILES` the removed
  implementation needed.

  Implementation: `_cloud.py`'s `_DatabricksPathCredential` (redacted `__repr__`/`__str__` —
  it's a field on every cloud `InputPartition` now, so it can appear in Spark's plan explain
  output or a worker traceback; redaction is how this design closes the exposure risk the
  *original*, pre-removal module docstring cited as the reason to avoid exactly this pattern),
  `_vend_databricks_path_credential()` (the raw REST call, stdlib `urllib.request`/`json`
  only — no new dependency), `_databricks_notebook_context()` (the `dbutils` token lookup —
  tries an IPython-injected `dbutils` first, falls back to constructing
  `pyspark.dbutils.DBUtils(spark)` explicitly; **the fallback construction path is unverified
  against a real workspace**, flagged in its own docstring — note also that open-source
  PySpark, unlike real Databricks Runtime, ships no `pyspark.dbutils` module at all, confirmed
  empirically, so this path is only ever reachable on Databricks itself), and
  `databricks_credential_for()` (driver-only in effect, cached per `(scheme, bucket)`,
  re-vends when missing or expired — a failed vend is *not* cached, so every call after a
  failure retries rather than permanently poisoning that bucket). `_base.py`'s
  `credential_for_partitions()`/`attach_credential()` are shared by all five format readers'
  `partitions()` to fetch-once-per-file and attach-to-every-partition-for-that-file.
  `_cloudfs.py`'s `filesystem_for()` picks up the active partition's credential via a new
  `_cloud.active_credential()` module-global (set for the duration of an enclosing
  `cloud_read_scope()`) — needed because some readers' worker-side code (`fastq.py`'s
  `_read_range()`, `bed.py`'s `_open_text()`) call `_cloudfs.open_stream()` directly,
  bypassing the pysam/htslib env-var path entirely, and would otherwise never see a vended
  credential at all. `fasta.py`'s driver-side `.fai` resolution (`_cloudfs.exists()`/
  `open_stream()`, called from `partitions()` before any `cloud_read_scope()` exists) gets
  this for free too, via `filesystem_for()`'s fallback to `databricks_credential_for(path) if
  is_databricks() else None` when `active_credential()` is `None`.

  **Refresh ceiling, stated explicitly, not silently accepted:** `partitions()` runs once, up
  front, before any partition executes, and workers have no channel to call back to the
  driver mid-read — so the finest achievable refresh granularity is "fresh as of the most
  recent `partitions()` call" (i.e. every new Spark action against a cloud path), not
  mid-read. A single action whose total wall-clock execution exceeds the vended credential's
  ~1h lifetime will still see mid-read expiry with no way to recover short of re-running the
  action. AWS/S3 only — GCS/Azure are not wired up via this mechanism (the same REST endpoint
  likely returns an analogous `gcp_oauth_token`/`azure_*` shape, matching what the removed
  `databricks-sdk`-based implementation used to parse, but that's unconfirmed through this raw
  REST path specifically and out of scope for this round). Unit-tested with everything mocked
  (no real workspace, no network) in `test_cloud_databricks_vending.py` — request/response
  shape, redaction, cache/expiry/re-vend behavior, and the `_cloudfs`/`prepare_env` wiring.
  `tests/smoke_uc_credential_vending.py` was rewritten for this architecture too (it must run
  as an actual notebook cell or Databricks Job task now, not via `databricks-connect` —
  Spark Connect's gateway-less client can't provide the real `dbutils` this mechanism needs
  driver-side; see that script's own docstring and `TESTING.md`'s new section). **Not yet
  re-run against a live workspace** — whether it actually authenticates end-to-end still
  needs that manual, pre-release check, same as the first attempt did before its own gaps
  surfaced.

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
- ~~Databricks UC credential vending, take one: whether `WorkspaceClient()`'s default auth
  resolves inside the isolated Python Data Source worker subprocess~~ — **resolved: no, it
  doesn't**, and that first implementation was removed as a result (a real worker-side
  diagnostic showed zero ambient Databricks credentials of any kind there, plus a
  `dbruntime` `ModuleNotFoundError`; see "Implemented since first cut" for the full finding).
  A second, differently-shaped implementation (vend once on the driver, thread plain
  credential values to workers) was built afterward specifically to route around this exact
  gap — see that section's "Re-implemented" entry. Two things about *that* implementation are
  open, unlike the first one's now-settled worker-auth question:
  - Whether `_databricks_notebook_context()`'s `pyspark.dbutils.DBUtils(spark)` fallback
    construction (used when there's no IPython-injected `dbutils` — e.g. a Job's Python
    script task rather than a notebook cell) actually resolves a working token on real
    Databricks compute — flagged as unverified in that function's own docstring.
  - Whether the whole mechanism authenticates end-to-end at all. `tests/smoke_uc_credential_
    vending.py` was rewritten for the new architecture but has not yet been run against a
    live workspace (see `TESTING.md`'s new section).
- Databricks sets `DATABRICKS_RUNTIME_VERSION` inside isolated worker subprocesses too, not
  just the driver — confirmed via a live worker-side diagnostic on dedicated/classic compute,
  during the now-removed first credential-vending attempt. Relevant again now that
  `is_databricks()` was reintroduced for the second attempt: `_cloudfs.filesystem_for()`
  calls it from worker-side code too, whenever `active_credential()` finds no scope-supplied
  credential — so a worker on Databricks with no active credential scope will still attempt
  `databricks_credential_for()`, which resolves to a cheap, local, no-network no-op there
  (`_active_spark_session()` fails closed with no JVM gateway to even try, before any REST
  call would happen — see that function's docstring), rather than skip straight to ambient
  resolution the way an off-Databricks worker does. Not a correctness issue and not a real
  performance concern (no network I/O involved), just worth knowing about if it ever needs
  explaining. Serverless uses a value shaped like `"client.N.M"` rather than classic DBR's
  plain numeric version (confirmed by a user directly, not independently verified via a
  worker-side diagnostic — Serverless doesn't expose `sparkContext.parallelize()`/`.map()`,
  so the same check isn't available there).
- `_cloud.py`'s cloud-read lock serializes all cloud-backed reads within one worker process
  (a deliberate correctness-over-parallelism tradeoff — see its module docstring) because
  it's unverified whether htslib re-reads env vars only at open time or per range-request.
  Revisit only if this is a measured bottleneck.
- Azure download-fallback (`_cloudfs.materialize_local`) caches by source URL for a worker
  process's lifetime but doesn't bound total local disk usage across distinct files — a
  long-running worker touching many large Azure files could accumulate significant local
  temp storage. Acceptable for typical partition-plan sizes; revisit if it becomes an issue.
