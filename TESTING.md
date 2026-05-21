# Testing Guide

> **All tests run inside Docker.** There is no local Maven/JDK requirement — every `mvn test` command below is executed inside a container. Run `docker compose build` once before running tests.

## Quick reference

| Command | What it tests |
|---|---|
| `docker compose run --rm spark-test bash -c "cd tests/smoke && mvn test -Pspark402"` | Pure Spark 4.x (Maven + JDK 17) |
| `docker compose run --rm spark402 bash -c "cd tests/smoke && mvn test -Pspark402"` | Apache Spark 4.0.2 runtime |
| `docker compose run --rm spark411 bash -c "cd tests/smoke && mvn test -Pspark411"` | Apache Spark 4.1.1 runtime |
| `docker compose run --rm databricks bash -c "cd tests/smoke && mvn test -Pspark402"` | Databricks Runtime 17.3-LTS |
| `docker compose run --rm databricks-uc bash -c "cd tests/smoke && mvn test -Pspark402"` | Databricks Runtime + Unity Catalog |
| `docker compose run --rm spark-uc bash -c "cd tests/smoke && mvn test -Pspark402"` | Pure Spark + Unity Catalog |
| `docker compose run --rm -e DATABRICKS_HOST=... -e DATABRICKS_TOKEN=... databricks-connect python3 tests/smoke_serverless.py` | Databricks serverless (real credentials needed) |
| `docker compose run --rm spark-test-s3` | S3 range-access tests against MinIO |

---

## Smoke test: SELECT 1

The smoke test is a minimal Maven project at `tests/smoke/` that creates a local `SparkSession` and asserts `SELECT 1 = 1`. It verifies that:
- The JVM can load Spark classes
- `SparkSession.builder().master("local[1]")` initialises correctly
- A basic SQL query executes and returns the expected result
- The Spark major version is 4.x

### Verified passing configurations

| Image | Maven profile | Spark version | Result |
|---|---|---|---|
| `spark-test` (maven:3.9-eclipse-temurin-17) | `spark402` | 4.0.2 | Tests run: 2, Failures: 0 |
| `spark402` (apache/spark:4.0.2) | `spark402` | 4.0.2 | Tests run: 2, Failures: 0 |
| `spark411` (apache/spark:4.1.1) | `spark411` | 4.1.1 | Tests run: 2, Failures: 0 |
| `databricks` (databricksruntime/standard:17.3-LTS) | `spark402` | 4.0.2 | Tests run: 2, Failures: 0 |

### Reproduce the smoke tests

```bash
# Build all images first (one-time, ~5-10 min):
docker compose build spark-test spark402 spark411 databricks

# Run all four smoke tests sequentially:
docker compose run --rm spark-test  bash -c "cd tests/smoke && mvn test -Pspark402  2>&1 | grep -E '(Tests run|BUILD|SMOKE)'"
docker compose run --rm spark402    bash -c "cd tests/smoke && mvn test -Pspark402  2>&1 | grep -E '(Tests run|BUILD|SMOKE)'"
docker compose run --rm spark411    bash -c "cd tests/smoke && mvn test -Pspark411  2>&1 | grep -E '(Tests run|BUILD|SMOKE)'"
docker compose run --rm databricks  bash -c "cd tests/smoke && mvn test -Pspark402  2>&1 | grep -E '(Tests run|BUILD|SMOKE)'"
```

---

## Full BAM reader tests

Once `pom.xml` exists at the project root, run the full test suite:

```bash
# Against the base Maven image (Spark 4.0.2):
docker compose run --rm spark-test mvn test -Pspark402

# Against a specific Spark runtime:
docker compose run --rm spark402 mvn test -Pspark402
docker compose run --rm spark411 mvn test -Pspark411

# Against Databricks Runtime (17.3-LTS = Spark 4.0.2):
docker compose run --rm databricks mvn test -Pspark402
```

---

## Unity Catalog integration tests

The `databricks-uc` and `spark-uc` services start a Unity Catalog OSS server
sidecar automatically (via `depends_on`).

```bash
# Start UC sidecar and run tests (Maven profile `uc-integration` to be defined in pom.xml):
docker compose run --rm databricks-uc mvn test -Pspark402,uc-integration
docker compose run --rm spark-uc mvn test -Pspark402,uc-integration

# Or start UC manually and inspect it:
docker compose up -d unity-catalog
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
```

---

## Cloud storage integration tests

Start the emulator sidecars, then run tests with the relevant Maven profile.

### S3 / s3a:// (MinIO)

```bash
docker compose up -d minio

# MinIO console: http://localhost:9001  (user: minioadmin / minioadmin)
# S3 endpoint for Spark: http://localhost:9000
# Test path format: s3a://test-bucket/sample.bam

# Create a bucket (requires mc or AWS CLI):
docker compose run --rm minio mc alias set local http://minio:9000 minioadmin minioadmin
docker compose run --rm minio mc mb local/test-bucket
```

Spark config needed in tests:
```
spark.hadoop.fs.s3a.endpoint=http://localhost:9000
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

### Azure ADLS Gen2 / abfss:// (Azurite)

```bash
docker compose up -d azurite

# Blob endpoint: http://localhost:10000/devstoreaccount1
# Connection string (for tooling):
#   DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;
#   AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
#   BlobEndpoint=http://localhost:10000/devstoreaccount1;
```

Spark config needed in tests:
```
spark.hadoop.fs.azure.account.auth.type.devstoreaccount1.dfs.core.windows.net=SharedKey
spark.hadoop.fs.azure.account.key.devstoreaccount1.dfs.core.windows.net=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

### Google Cloud Storage / gs:// (fake-gcs-server)

```bash
docker compose up -d fake-gcs

# GCS endpoint: http://localhost:4443
```

Spark config needed in tests:
```
spark.hadoop.fs.gs.storage.root.url=http://localhost:4443
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
```

---

## Range-request verification tests (Option B — MinIO)

These tests confirm that indexed reads (`.bai`, `.fai`, `.crai`) issue HTTP **Range**
requests to object storage rather than downloading the entire file. If range requests
are absent, every read is a full-file transfer — defeating the purpose of the index.

### Implementation

Three JUnit test classes live in `core/src/test-s3/java/` (compiled only with
`-Ps3-integration`) and run against a real MinIO instance:

| Test class | Index | What it verifies |
|---|---|---|
| `BaiRangeS3Test` | `.bai` | Region query transfers fewer S3 bytes than a full scan |
| `FaiRangeS3Test` | `.fai` | Single-contig read transfers fewer bytes than the full FASTA |
| `CraiRangeS3Test` | `.cram.crai` | CRAI-guided read does not exceed file size; ≤ no-index scan |

Each class:
- Uploads its fixtures to MinIO in `@BeforeAll` (no `mc` required — uses the AWS SDK v1
  bundled with `hadoop-aws`)
- Configures S3A with `fs.s3a.readahead.range=0` so every htsjdk seek maps to its own
  HTTP Range request with no prefetch inflation
- Snapshots `FileSystem.getAllStatistics()` (S3A scheme) before and after each operation
  to compute a per-test bytes-transferred delta
- Cleans up its objects in `@AfterAll`

Tests skip automatically (via `assumeTrue`) when `s3.endpoint` is not set, so they
never block the default `mvn test` run.

### Running the tests

**Quickest path — Docker Compose (recommended):**

```bash
# Build the image if you haven't already:
docker compose build spark-test

# Start MinIO, then run S3 range tests (fixtures uploaded by the tests themselves):
docker compose run --rm spark-test-s3
```

**Against a local MinIO (without Docker Compose):**

```bash
# Start MinIO:
docker compose up -d minio

# Run from your workstation (JDK 17 + Maven required locally):
mvn test -pl core -Pspark402,s3-integration \
  -Ds3.endpoint=http://localhost:9000 \
  -Ds3.bucket=test-bucket
```

**Run a single test class:**

```bash
docker compose run --rm spark-test-s3 bash -c \
  "mvn test -pl core -Pspark402,s3-integration \
    -Ds3.endpoint=http://minio:9000 \
    -Dtest=BaiRangeS3Test"
```

### Verifying Range headers in MinIO logs

MinIO is configured with `--json` logging, which emits structured JSON for every S3
request. After running the S3 tests, inspect the logs to confirm `Range` headers:

```bash
# All Range headers seen by MinIO during the test run:
docker compose logs minio | grep -o '"Range":"[^"]*"'

# Expected output (BAM region query example):
# "Range":"bytes=0-359"         ← BAI index fetch
# "Range":"bytes=4096-8191"     ← BGZF block for CHROMOSOME_I reads
# "Range":"bytes=8192-10239"    ← next BGZF block
```

The absence of a `Range: bytes=0-<fileSize>` full-file request confirms that
BAI/FAI/CRAI-guided reads are using targeted seeks rather than streaming from the start.

### What the tests assert

**`BaiRangeS3Test`**

| Test | Assertion |
|---|---|
| `fullScan_correctCount` | `count == 112` (correctness) |
| `regionQuery_correctCount` | `count == 18` for `CHROMOSOME_I` (correctness) |
| `regionQuery_readsFewerBytesThanFullScan` | `regionDeltaBytes < fullScanDeltaBytes` AND `regionDeltaBytes < bamFileSize` |

**`FaiRangeS3Test`** (synthetic 5-contig FASTA, 300 bases × 5 = 1 500 bases)

| Test | Assertion |
|---|---|
| `indexedRead_contigCount` | `count == 5` (correctness) |
| `indexedRead_contigLengthCorrect` | `length == 300` for `chr3` (correctness) |
| `indexedContigRead_readsFewerBytesThanFileSize` | `contigDeltaBytes < totalFastaFileSize` |

**`CraiRangeS3Test`** (synthetic 10-record CRAM, one chromosome)

| Test | Assertion |
|---|---|
| `fullScan_correctCount` | `count == 10` (correctness) |
| `regionQuery_correctCount` | `count == 10` for `chr1` (correctness) |
| `cramRead_bytesNotExceedFileSize` | `transferredBytes ≤ cramFileSize + 4096` |
| `craiGuidedRead_readsFewerBytesThanNoIndexScan` | `indexedBytes ≤ fullScanBytes + 4096` |

### How the bytes-transferred metric works

`FileSystem.Statistics.getBytesRead()` accumulates across all reads on the S3A scheme
within the JVM. Each test snapshots the counter before the operation and computes the
delta — so prior test runs don't pollute the comparison. With `readahead.range=0`, the
delta directly reflects the HTTP bytes MinIO delivered for that specific operation.

---

## Databricks Serverless (databricks-connect)

Databricks Serverless runs on a real Databricks workspace — there is no local emulator.
You need a Databricks personal access token or OAuth credentials.

```bash
# Personal access token (PAT):
docker compose run --rm \
  -e DATABRICKS_HOST=https://<workspace>.azuredatabricks.net \
  -e DATABRICKS_TOKEN=<token> \
  databricks-connect python3 tests/smoke_serverless.py

# OAuth Machine-to-Machine (M2M):
docker compose run --rm \
  -e DATABRICKS_HOST=https://<workspace>.azuredatabricks.net \
  -e DATABRICKS_CLIENT_ID=<client-id> \
  -e DATABRICKS_CLIENT_SECRET=<client-secret> \
  databricks-connect python3 tests/smoke_serverless.py
```

The test script (`tests/smoke_serverless.py`) creates a serverless
`DatabricksSession` and runs `SELECT 1`. It exits 0 on success, 1 on failure.

**Note on JAR deployment for serverless jobs:**
Databricks Serverless workflows require JARs to be uploaded to a Unity Catalog
Volume or workspace files — not `dbfs:/`. Build the serverless-compatible JAR
with the `serverless` Maven profile (to be defined in `pom.xml`):

```bash
docker compose run --rm serverless-build
# → target/lite-bfx-spark-*-serverless.jar
```

Then upload via Databricks CLI:
```bash
databricks fs cp target/lite-bfx-spark-*-serverless.jar \
  dbfs:/FileStore/jars/lite-bfx-spark.jar
# Or to Unity Catalog volume:
databricks fs cp target/lite-bfx-spark-*-serverless.jar \
  /Volumes/<catalog>/<schema>/<volume>/lite-bfx-spark.jar
```

---

## Updating samtools version

All images pin samtools via the `SAMTOOLS_VERSION` build arg (default: `1.21`).
To rebuild with a different version:

```bash
docker compose build --build-arg SAMTOOLS_VERSION=1.21 spark-test
# Or for all images:
SAMTOOLS_VERSION=1.21 docker compose build
```

Pass `SAMTOOLS_VERSION` in your `docker compose build` or set it in a `.env` file.
