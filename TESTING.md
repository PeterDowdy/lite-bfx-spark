# Testing Guide

## Quick reference

| Command | What it tests |
|---|---|
| `docker compose run --rm spark-test bash -c "cd tests/smoke && mvn test"` | Pure Spark 4.x (Maven + JDK 17) |
| `docker compose run --rm spark402 bash -c "cd tests/smoke && mvn test"` | Apache Spark 4.0.2 runtime |
| `docker compose run --rm spark411 bash -c "cd tests/smoke && mvn test"` | Apache Spark 4.1.1 runtime |
| `docker compose run --rm databricks bash -c "cd tests/smoke && mvn test"` | Databricks Runtime 17.3-LTS |
| `docker compose run --rm databricks-uc bash -c "cd tests/smoke && mvn test"` | Databricks Runtime + Unity Catalog |
| `docker compose run --rm spark-uc bash -c "cd tests/smoke && mvn test"` | Pure Spark + Unity Catalog |
| `docker compose run --rm -e DATABRICKS_HOST=... -e DATABRICKS_TOKEN=... databricks-connect python3 tests/smoke_serverless.py` | Databricks serverless (real credentials needed) |

---

## Smoke test: SELECT 1

The smoke test is a minimal Maven project at `tests/smoke/` that creates a local `SparkSession` and asserts `SELECT 1 = 1`. It verifies that:
- The JVM can load Spark classes
- `SparkSession.builder().master("local[1]")` initialises correctly
- A basic SQL query executes and returns the expected result
- The Spark major version is 4.x

### Verified passing configurations

| Image | Spark version | Result |
|---|---|---|
| `spark-test` (maven:3.9-eclipse-temurin-17) | 4.0.2 | Tests run: 2, Failures: 0 |
| `spark402` (apache/spark:4.0.2) | 4.0.2 | Tests run: 2, Failures: 0 |
| `spark411` (apache/spark:4.1.1) | 4.0.2* | Tests run: 2, Failures: 0 |
| `databricks` (databricksruntime/standard:17.3-LTS) | 4.0.2 | Tests run: 2, Failures: 0 |

*spark411 uses Spark 4.0.2 from Maven Central in the smoke pom.xml; the `sparkVersion` test asserts major == 4, which holds.

### Reproduce the smoke tests

```bash
# Build all images first (one-time, ~5-10 min):
docker compose build spark-test spark402 spark411 databricks

# Run all four smoke tests sequentially:
for svc in spark-test spark402 spark411 databricks; do
  echo "=== $svc ==="
  docker compose run --rm "$svc" bash -c "cd tests/smoke && mvn test 2>&1 | grep -E '(Tests run|BUILD|SMOKE)'"
done
```

---

## Full BAM reader tests

Once `pom.xml` exists at the project root, run the full test suite:

```bash
# Against the base Maven image:
docker compose run --rm spark-test mvn test

# Against a specific Spark runtime:
docker compose run --rm spark402 mvn test
docker compose run --rm spark411 mvn test

# Against Databricks Runtime:
docker compose run --rm databricks mvn test
```

---

## Unity Catalog integration tests

The `databricks-uc` and `spark-uc` services start a Unity Catalog OSS server
sidecar automatically (via `depends_on`).

```bash
# Start UC sidecar and run tests (Maven profile `uc-integration` to be defined in pom.xml):
docker compose run --rm databricks-uc mvn test -Puc-integration
docker compose run --rm spark-uc mvn test -Puc-integration

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
