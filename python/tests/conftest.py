"""Shared test fixtures. Reuses the JAR's checked-in fixtures under core/src/test/resources."""

import collections
import os

import pytest

# conftest.py always loads before any test file, so this applies litebfx's Databricks
# FIPS-crash workaround (see _base.import_pysam()'s docstring) exactly once, process-wide,
# before any test file's own `import pysam` runs -- test code doesn't go through litebfx's
# internal call sites, so without this it would crash on a real Databricks image the same
# way an unpatched `import pysam` would (confirmed: this is exactly how
# python-test-databricks first caught this gap).
try:
    from litebfx._base import import_pysam
    import_pysam()
except ImportError:
    pass    # litebfx (or pysam) not installed yet -- fine, individual tests importorskip

_HERE = os.path.dirname(__file__)
_RES = os.path.abspath(os.path.join(_HERE, "..", "..", "core", "src", "test", "resources"))


def resource(name):
    return os.path.join(_RES, name)


def norm(row):
    """Normalize a Spark Row / tuple to a hashable, order-stable form for multiset compares."""
    vals = list(row)
    out = []
    for v in vals:
        if isinstance(v, dict):
            out.append(tuple(sorted(v.items())))
        elif isinstance(v, (list, tuple)):
            out.append(tuple(v))
        else:
            out.append(v)
    return tuple(out)


def multiset(rows):
    return collections.Counter(norm(r) for r in rows)


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("litebfx-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    import litebfx
    litebfx.register_all(s)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def s3_bucket():
    """Uploads range.bam/.bai and realn01.fa/.fai to S3; yields the s3://bucket/prefix base
    URI. Skipped (not failed) when S3_BUCKET is unset -- run via `docker compose run --rm
    python-test-s3` (MinIO) or `python-test-s3-live` (real AWS), not in default CI.

    Credentials are never read here directly: boto3's own default chain resolves
    AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (or instance profile / shared config) the same
    way htslib and pyarrow.fs do, so MinIO's fake keys and a real IAM user's keys both just
    work via the same env vars, set in docker-compose.yml / .env -- nothing here needs to
    know which one it is.
    """
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        pytest.skip("S3_BUCKET not set -- run via docker compose run --rm python-test-s3 "
                     "(MinIO) or python-test-s3-live (real AWS)")
    boto3 = pytest.importorskip("boto3")
    endpoint = os.environ.get("S3_ENDPOINT")    # unset -> real AWS default endpoint
    s3 = boto3.client("s3", **({"endpoint_url": endpoint} if endpoint else {}))
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass    # already exists (BucketAlreadyOwnedByYou on MinIO reruns; on a real bucket
                # this identity may not even have s3:CreateBucket, which is fine -- it
                # already exists, per the least-privilege policy in TESTING.md)
    prefix = "litebfx-test"
    # INTELLIGENT_TIERING on a real bucket only: a cost safety net in case these fixtures
    # are ever left behind and forgotten -- objects that go untouched automatically move to
    # cheaper access tiers over time instead of sitting at STANDARD indefinitely. MinIO
    # rejects it outright (InvalidStorageClass, confirmed empirically -- it validates
    # against its own small supported set, unlike silently ignoring an unknown class), so
    # this only applies when there's no custom endpoint (i.e. real AWS).
    extra_args = {} if endpoint else {"ExtraArgs": {"StorageClass": "INTELLIGENT_TIERING"}}
    for name in ("range.bam", "range.bam.bai", "realn01.fa", "realn01.fa.fai"):
        s3.upload_file(resource(name), bucket, f"{prefix}/{name}", **extra_args)
    yield f"s3://{bucket}/{prefix}"


@pytest.fixture(scope="session")
def gcs_bucket():
    """Uploads the same fixtures as s3_bucket, to fake-gcs-server via its REST API directly
    (no google-cloud-storage client dependency needed, matching the Java side's
    spark-test-gcs approach). Skipped when GCS_ENDPOINT is unset."""
    endpoint = os.environ.get("GCS_ENDPOINT")
    if not endpoint:
        pytest.skip("GCS_ENDPOINT not set -- run via docker compose run --rm python-test-gcs")
    import urllib.request
    bucket = os.environ.get("GCS_BUCKET", "test-bucket")
    req = urllib.request.Request(
        f"{endpoint}/storage/v1/b", method="POST",
        data=f'{{"name": "{bucket}"}}'.encode(), headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
    except Exception:
        pass    # already exists on repeat local runs
    prefix = "litebfx-test"
    for name in ("range.bam", "range.bam.bai", "realn01.fa", "realn01.fa.fai"):
        with open(resource(name), "rb") as fh:
            data = fh.read()
        url = f"{endpoint}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={prefix}/{name}"
        urllib.request.urlopen(urllib.request.Request(url, method="POST", data=data))
    yield f"gs://{bucket}/{prefix}"


@pytest.fixture(scope="session")
def gcs_bucket_live():
    """Uploads the same fixtures as gcs_bucket, to a real GCS bucket instead of fake-gcs-
    server -- the only way to test htslib's actual native gs:// read path at all. Unlike S3,
    htslib's GCS backend has zero endpoint-override capability (confirmed by binary
    inspection), so fake-gcs-server can only ever exercise the pyarrow.fs orchestration
    layer (see gcs_bucket / test_cloud_gcs.py); this is the real thing. Skipped when
    GOOGLE_APPLICATION_CREDENTIALS is unset -- run via `docker compose run --rm
    python-test-gcs-live` (or python-test-databricks-gcs-live), not in default CI.

    Two credential consumers, two different things needed from the same service-account
    key -- unlike S3, where one long-lived AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY pair
    serves both boto3/pyarrow.fs *and* htslib:
    - pyarrow.fs.GcsFileSystem's ambient resolution (used for the fixture upload here, and
      by _cloudfs.py's orchestration layer at read time) wants GOOGLE_APPLICATION_CREDENTIALS
      pointing at the key file directly -- standard Application Default Credentials.
    - htslib's native GCS backend wants an already-minted OAuth access token in
      GCS_OAUTH_TOKEN, not the key file itself. Minted here from the same key, refreshed
      once per test session (~1h token lifetime) -- litebfx itself has no equivalent ambient
      minting today (see TASKS.md); this fixture only covers the test-time need.
    """
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        pytest.skip("GOOGLE_APPLICATION_CREDENTIALS not set -- run via docker compose run "
                     "--rm python-test-gcs-live")
    google_auth = pytest.importorskip("google.auth")
    import google.auth.transport.requests
    import pyarrow.fs

    bucket = os.environ["GCS_BUCKET"]
    credentials, _ = google_auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_write"])
    credentials.refresh(google.auth.transport.requests.Request())
    os.environ["GCS_OAUTH_TOKEN"] = credentials.token

    fs = pyarrow.fs.GcsFileSystem()   # ambient: reads GOOGLE_APPLICATION_CREDENTIALS itself
    prefix = "litebfx-test"
    for name in ("range.bam", "range.bam.bai", "realn01.fa", "realn01.fa.fai"):
        with open(resource(name), "rb") as src, fs.open_output_stream(
                f"{bucket}/{prefix}/{name}") as dst:
            dst.write(src.read())
    yield f"gs://{bucket}/{prefix}"


@pytest.fixture(scope="session")
def azure_container():
    """Uploads the same fixtures as s3_bucket, to Azurite via pyarrow.fs.AzureFileSystem
    directly (it can write too, so no azure-storage-blob client dependency needed). Uses
    Azurite's well-known dev account/key -- the same ones this repo's docker-compose.yml
    already documents for the Java side's spark-test-azure. Skipped when
    AZURE_STORAGE_ENDPOINT_URL is unset."""
    endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("AZURE_STORAGE_ENDPOINT_URL not set -- run via docker compose run --rm "
                     "python-test-azure")
    import urllib.parse
    import pyarrow.fs
    account = os.environ.get("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "test-container")
    parsed = urllib.parse.urlparse(endpoint)
    fs = pyarrow.fs.AzureFileSystem(
        account_name=account, account_key=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        blob_storage_authority=parsed.netloc, dfs_storage_authority=parsed.netloc,
        blob_storage_scheme=parsed.scheme or "http", dfs_storage_scheme=parsed.scheme or "http")
    try:
        fs.create_dir(container)
    except Exception:
        pass    # already exists on repeat local runs
    prefix = "litebfx-test"
    for name in ("range.bam", "range.bam.bai", "realn01.fa", "realn01.fa.fai"):
        with open(resource(name), "rb") as src, fs.open_output_stream(
                f"{container}/{prefix}/{name}") as dst:
            dst.write(src.read())
    yield f"abfss://{container}@{account}.dfs.core.windows.net/{prefix}"


@pytest.fixture(scope="session")
def vcf_fixture(tmp_path_factory):
    """A tiny bgzipped + tabix-indexed VCF (no VCF fixture is checked into the repo)."""
    import pysam
    d = tmp_path_factory.mktemp("vcf")
    plain = str(d / "calls.vcf")
    with open(plain, "w") as fh:
        fh.write(
            "##fileformat=VCFv4.2\n"
            '##FILTER=<ID=PASS,Description="passed">\n'
            '##FILTER=<ID=q10,Description="low qual">\n'
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="depth">\n'
            '##INFO=<ID=AF,Number=A,Type=Float,Description="af">\n'
            '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbsnp">\n'
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
            '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="dp">\n'
            "##contig=<ID=chr1>\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n"
            "chr1\t100\trs1\tA\tT\t50\tPASS\tDP=10;AF=0.5;DB\tGT:DP\t0/1:9\t1|1:8\n"
            "chr1\t200\t.\tG\tC,T\t.\tq10\tDP=3\tGT:DP\t0/0:2\t./.:.\n"
        )
    bgz = plain + ".gz"
    pysam.tabix_compress(plain, bgz, force=True)
    pysam.tabix_index(bgz, preset="vcf", force=True)
    return bgz
