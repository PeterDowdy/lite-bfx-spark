# Databricks Unity Catalog credential vending smoke test: reads a real BAM directly from
# s3://, gs://, or abfss:// via litebfx, against a real UC-governed external location.
"""

This is the one thing that cannot be verified without a real workspace: whether
WorkspaceClient()'s default auth resolves inside the *isolated Python Data Source worker
subprocess* that runs partitions()/read() (vs. only the driver, which is well-established to
work). See litebfx/_cloud.py's module docstring and python/TASKS.md's open questions.

Prerequisites:
  - DATABRICKS_HOST env var  (e.g. https://my-workspace.azuredatabricks.net)
  - DATABRICKS_TOKEN env var (personal access token)
    OR DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M)
  - UC_TEST_BAM_URL env var: a direct s3://, gs://, or abfss:// URL to a small BAM under a
    Unity Catalog External Location the identity above is granted READ FILES on. A co-located
    or indexPath-resolvable .bai is optional but recommended (exercises the region-query path
    too, not just a full scan).
  - databricks-sdk installed (pip install databricks-sdk), so litebfx._cloud can actually
    call the Temporary Credentials API. Not a litebfx extra -- see pyproject.toml's comment
    on why -- but this script runs via databricks-connect on a local/CI machine, not real
    Databricks compute, so unlike there, it genuinely isn't preinstalled here and must be
    added explicitly.

Run via the databricks-connect-uc service (installs litebfx + databricks-sdk from the local
checkout on top of the databricks-connect image -- needed for the actual credential-vending
call, not just the Spark Connect client):
  docker compose run --rm \\
    -e DATABRICKS_HOST=https://<workspace>.azuredatabricks.net \\
    -e DATABRICKS_TOKEN=<token> \\
    -e UC_TEST_BAM_URL=s3://my-bucket/genomics/sample.bam \\
    databricks-connect-uc
"""

import os
import sys

try:
    from databricks.connect import DatabricksSession
except ImportError:
    print("ERROR: databricks-connect not installed. Run inside the databricks-connect service.")
    sys.exit(1)

host = os.environ.get("DATABRICKS_HOST")
token = os.environ.get("DATABRICKS_TOKEN")
bam_url = os.environ.get("UC_TEST_BAM_URL")

if not host:
    print("ERROR: DATABRICKS_HOST environment variable not set.")
    sys.exit(1)
if not token and not (os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET")):
    print("ERROR: DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET) not set.")
    sys.exit(1)
if not bam_url:
    print("ERROR: UC_TEST_BAM_URL not set -- point it at a small BAM under a UC External "
          "Location the connecting identity has READ FILES on (s3://, gs://, or abfss://).")
    sys.exit(1)

print(f"Connecting to: {host}")
print(f"Reading (full scan): {bam_url}")

spark = DatabricksSession.builder.serverless(True).getOrCreate()

import litebfx    # noqa: E402  (after DatabricksSession init, matching smoke_serverless.py's flow)
litebfx.register_all(spark)

count = spark.read.format("bam").load(bam_url).count()
assert count > 0, f"Expected at least one read, got {count} -- empty file, or the read " \
    "silently returned nothing instead of raising (check driver logs for a swallowed error)."

print(f"[SMOKE] UC credential vending PASSED  ({count} reads, Spark {spark.version})")
print("This confirms WorkspaceClient() auth resolves inside the Python Data Source worker "
      "subprocess on this workspace -- see litebfx/_cloud.py's module docstring.")
spark.stop()
