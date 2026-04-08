# Cloud storage

All file I/O in lite-bfx-spark goes through Hadoop `FileSystem`. This means any storage backend that Spark already supports — S3, Azure ADLS Gen2, Google Cloud Storage, DBFS, Unity Catalog Volumes — works without any library-specific configuration.

---

## How it works

The library wraps Hadoop `FSDataInputStream` in an htsjdk `SeekableStream` (`HadoopSeekableStream`). htsjdk uses this stream to seek to specific byte offsets within files — for example, to the BGZF block indicated by a BAI virtual file offset. The Hadoop `FileSystem` translates those seeks into HTTP `Range` requests when the file lives on object storage.

This means a region query on a BAM file stored in S3 looks like:

1. Driver fetches the BAI (small sequential download, ~360 bytes for a small BAM)
2. Driver computes the VFO ranges for the requested region
3. Executors issue `Range: bytes=<start>-<end>` HTTP requests directly to S3 — only the BGZF blocks that contain the requested reads are transferred

No full-file download occurs on either the driver or executor.

---

## Credential propagation

Spark's `hadoopConfiguration` holds the credentials for cloud storage (IAM role ARN, access keys, OAuth tokens, service account keys). The library captures this configuration on the driver when planning partitions and stores a serialized copy inside each `InputPartition` (`SerializableConfiguration`).

When Spark sends a partition to an executor, the credentials travel with it. The executor reconstructs the `FileSystem` from the serialized configuration to open the file. This is important: if credentials were not propagated this way, executors would need to fetch them independently (via instance metadata, environment variables, etc.) which may not be available in all cluster configurations.

---

## Amazon S3 (s3a://)

S3A is the recommended S3 connector for Spark. On EMR, Databricks, and GKE clusters with IAM roles, no explicit credential configuration is needed — the cluster's instance profile or service account provides credentials automatically.

For explicit credentials:

```python
spark.conf.set("spark.hadoop.fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
```

For a path-style endpoint (MinIO or other S3-compatible):

```python
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Read from S3:

```python
df = spark.read.format("bam") \
    .option("indexDir", "s3a://my-indexes/cohort/") \
    .load("s3a://my-data/cohort/")
```

### Disabling S3A prefetching

By default, S3A prefetches ahead of each read. This can inflate the bytes-read count for index-guided reads. To measure true range-request efficiency (or to minimize prefetch overhead for small BAI-guided reads):

```python
spark.conf.set("spark.hadoop.fs.s3a.readahead.range", "0")
```

---

## Azure ADLS Gen2 (abfss://)

On Azure HDInsight and Databricks on Azure, credentials are pre-configured via the cluster's managed identity or service principal.

For explicit OAuth credentials:

```python
account = "mystorageaccount"
spark.conf.set(
    f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"spark.hadoop.fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"spark.hadoop.fs.azure.account.oauth2.client.id.{account}.dfs.core.windows.net",
    "<client-id>"
)
spark.conf.set(
    f"spark.hadoop.fs.azure.account.oauth2.client.secret.{account}.dfs.core.windows.net",
    "<client-secret>"
)
spark.conf.set(
    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{account}.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
)
```

Read:

```python
df = spark.read.format("bam").load(
    f"abfss://container@{account}.dfs.core.windows.net/data/sample.bam"
)
```

For the Azurite emulator (local development):

```python
account = "devstoreaccount1"
key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
spark.conf.set(f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net", key)
```

---

## Google Cloud Storage (gs://)

On GKE and Dataproc, Workload Identity or the node's service account provides credentials automatically. For explicit configuration:

```python
spark.conf.set("spark.hadoop.fs.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.json.keyfile",
    "/path/to/service-account.json")
```

Read:

```python
df = spark.read.format("bam").load("gs://my-bucket/data/sample.bam")
```

---

## Databricks DBFS (dbfs:/)

DBFS is pre-configured on all Databricks clusters. No credential setup is needed.

```python
df = spark.read.format("bam").load("dbfs:/mnt/genomics/sample.bam")
```

DBFS mounts (`/mnt/...`) work identically — the DBFS `FileSystem` implementation translates them to the underlying cloud storage path.

---

## Databricks Unity Catalog Volumes

Unity Catalog Volumes are accessible via FUSE mount on DBR 13+. The path format is `/Volumes/<catalog>/<schema>/<volume>/`:

```python
df = spark.read.format("bam").load(
    "/Volumes/genomics/prod/alignments/sample.bam"
)
```

For serverless Databricks jobs, JARs must live in a Unity Catalog Volume or workspace files — not `dbfs:/`. See [README](../README.md#deploying-to-a-cluster) for deployment details.

---

## Local filesystem

For local development and testing:

```python
# Absolute path
df = spark.read.format("bam").load("/home/user/data/sample.bam")

# file:// URI
df = spark.read.format("bam").load("file:///home/user/data/sample.bam")
```

---

## Verifying range requests

When running against MinIO (included in the test Docker Compose setup), you can inspect the HTTP requests MinIO received to confirm the library is issuing `Range` requests rather than full-file downloads:

```bash
docker compose logs minio | grep -o '"Range":"[^"]*"'
```

Expected output for a BAI-guided region query:

```
"Range":"bytes=0-359"        ← BAI index fetch
"Range":"bytes=4096-8191"    ← BGZF block containing the requested reads
"Range":"bytes=8192-10239"   ← next BGZF block
```

The absence of a `Range: bytes=0-<fileSize>` line confirms that BAI-guided reads are not downloading the whole file.

The S3 integration test suite (`-Ps3-integration`) formally asserts this by comparing bytes-transferred deltas for region queries versus full scans. See [TESTING.md](../TESTING.md) for how to run these tests.
