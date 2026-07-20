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

`pysam` is the only hard dependency. `pyspark` and `pyarrow` are deliberately left out of the
dependency graph entirely — they're already present in every environment this package
actually runs in (any Spark cluster; Databricks Runtime/Serverless specifically bundles a
runtime-pinned `pyarrow` tightly coupled to internal machinery), and declaring them risks pip
reconciling against those pins and conflicting — a real, observed install failure on
Databricks dedicated compute, not a hypothetical one (see `pyproject.toml`'s comments for the
details). One optional extra remains: `gcp`, for minting a GCS access token from an ambient
`GOOGLE_APPLICATION_CREDENTIALS` service-account key:

```bash
pip install "lite-bfx-spark[gcp]"
```

If installing on Databricks dedicated compute still hits a dependency conflict — some other
package that environment has differently pinned — install with `--no-deps` to skip dependency
resolution entirely, then just ensure `pysam` (the one dependency Databricks never bundles) is
present:

```bash
pip install --no-deps "lite-bfx-spark @ git+https://github.com/PeterDowdy/lite-bfx-spark.git@main#subdirectory=python"
pip install pysam>=0.22
```

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

## Cloud storage

`s3://`, `s3a://`, and `gs://` are read **directly** — no FUSE mount needed. pysam's bundled
htslib has a native S3/GCS remote-read backend, so `spark.read.format("bam").load("s3://
bucket/sample.bam")` just works, with the same index-guided range-request behavior as a local
file. `abfss://`/`wasbs://` are also read directly, but differently: htslib has no native
Azure backend, so the object is downloaded to a local temp file first and pysam reads that —
this gets you direct Azure support with no mount, at the cost of the range-request-only
efficiency S3/GCS get (the whole file transfers once per worker process, then reads from it
are as fast as local disk, including index-guided seeks within the downloaded copy).

**Credentials** are ambient (env vars, instance profile / workload identity, shared config —
the same resolution any AWS/GCS/Azure SDK tool uses) — for GCS and Azure everywhere, and for
S3 everywhere off Databricks. **On Databricks, `s3://` paths get an automatically-vended
Unity Catalog path credential instead**: the driver fetches a short-lived AWS credential from
UC's Temporary Path Credentials API (via `dbutils`'s own notebook-context token, not
`databricks-sdk` — this package still doesn't depend on it) and threads it to the worker that
needs it. The only setup required is granting the running identity `EXTERNAL USE LOCATION` on
the external location backing the path. This is a second, differently-shaped attempt at
Databricks credential vending — a first version (workers calling Unity Catalog's API
directly) was built and then removed after confirming it cannot authenticate from inside the
isolated Python Data Source worker subprocess at all; this one avoids that problem by never
asking a worker to authenticate to Databricks in the first place. See `python/TASKS.md` for
the full history of both attempts.

GCS and Azure aren't covered by this vending mechanism yet, so on Databricks Serverless
specifically (which has no instance-profile equivalent for ambient credentials) those two
clouds still need credentials made ambient some other way (e.g. a service principal's
`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET` injected into the compute environment) or a
Unity Catalog Volume path instead. A classic, instance-profile-configured cluster is
unaffected for any of the three clouds — ambient resolution works there the same as off
Databricks.

**GCS is a partial exception to "ambient just works."** htslib's native GCS backend needs an
already-minted OAuth *access token* in `GCS_OAUTH_TOKEN`, not a key file path — so the most
common ambient GCS credential (`GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account
key, standard Application Default Credentials) isn't directly usable by itself. Install the
`gcp` extra and this package mints a token from that key file automatically (cached and
refreshed as it nears expiry); without the extra, `gs://` reads fail with a permission error
even though the key file is right there. S3 and Azure don't have this gap — their ambient
credential shapes are exactly what htslib/`pyarrow.fs` already expect.

Everything else — `adl://`, `hdfs://`, `http(s)://`, `ftp://` — still needs a mount:

- **On Databricks**, use a **Unity Catalog Volume** path (`/Volumes/<cat>/<schema>/<vol>/…`,
  works on classic and serverless) or a `dbfs:/…` path — Databricks does the cloud I/O and
  auth under the mount. `dbfs:/` is normalized to `/dbfs/`. UC Volumes also work as a matter
  of course for `s3://`/`gs://`/`abfss://` if you'd rather not manage direct-path permissions.
- **Off Databricks**, mount the bucket yourself (`mountpoint-s3`, `s3fs`, `gcsfuse`,
  `blobfuse2`, `rclone mount`) and pass the mounted path.

## Known issues

**Databricks Runtime: `import pysam` crash.** Databricks Runtime sets
`OPENSSL_FORCE_FIPS_MODE` (e.g. `"0"`) in the process environment for its own bundled
OpenSSL. pysam's wheel vendors a separate, relocated OpenSSL 1.1.1 build that also reads
that variable — its mere presence, regardless of value, triggers a FIPS self-test that
fails on the relocated binary and aborts the process (`FATAL FIPS SELFTEST FAILURE`). This
package works around it automatically — `litebfx._base.import_pysam()` strips the variable
before the first `import pysam` — so no user action is needed.

**Debian/Ubuntu images — including Databricks Runtime: real `s3://`/`gs://` reads fail with a
misleading "No such file or directory."** pysam's manylinux wheel bundles a libcurl built
expecting the CA bundle at the RedHat/CentOS path (`/etc/pki/tls/certs/ca-bundle.crt`),
compiled in via `CURLOPT_CAINFO` — not something `CURL_CA_BUNDLE`/`SSL_CERT_FILE` can
override at runtime, since htslib calls libcurl's C API directly rather than going through
the `curl` CLI tool (which is what actually honors those env vars). Debian/Ubuntu — this
includes Databricks Runtime, confirmed on 17.3-LTS, it is not exempt — keeps the real bundle
at `/etc/ssl/certs/ca-certificates.crt` instead, so any genuine HTTPS connection through
pysam's bundled libcurl (real S3/GCS — a plain-HTTP test emulator like MinIO never exercises
this path) fails the TLS handshake, and htslib reports it as a generic "No such file or
directory" rather than a certificate error.

This package works around it automatically where it can — `litebfx._cloud.prepare_env()`
symlinks the expected path to the real bundle on first native S3/GCS open, best-effort and
silent on failure — but that needs write access to `/etc/`, which isn't guaranteed (a
non-root process, a read-only filesystem). If your environment doesn't allow it, fix it once
in your image instead:
```dockerfile
RUN apt-get install -y ca-certificates \
    && mkdir -p /etc/pki/tls/certs \
    && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
```
(`docker/Dockerfile.python` in this repo does exactly this, as belt-and-suspenders alongside
the runtime fix.)

## Relationship to the JAR

The JAR and this package are two implementations of the same reader for two deployment
models — install **one**, not both. Use the JAR when you need optimizer integration
(statistics, ordering), byte-balanced splitting, or Hadoop-native access to a cloud scheme
this package doesn't cover directly (`adl://`, `hdfs://`); use this package when you want a
`pip install` with no JVM library to provision — S3, GCS, and Azure are all direct here too.

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
