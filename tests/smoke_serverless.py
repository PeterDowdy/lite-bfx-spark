# Databricks Connect serverless smoke test: SELECT 1 against a real Databricks workspace.
"""

Prerequisites:
  - DATABRICKS_HOST env var  (e.g. https://my-workspace.azuredatabricks.net)
  - DATABRICKS_TOKEN env var (personal access token)
    OR DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M)

Run inside the databricks-connect container:
  docker compose run --rm \\
    -e DATABRICKS_HOST=https://<workspace>.azuredatabricks.net \\
    -e DATABRICKS_TOKEN=<token> \\
    databricks-connect python3 /workspace/tests/smoke_serverless.py
"""

import sys
import os

try:
    from databricks.connect import DatabricksSession
except ImportError:
    print("ERROR: databricks-connect not installed. Run inside the databricks-connect service.")
    sys.exit(1)

host = os.environ.get("DATABRICKS_HOST")
token = os.environ.get("DATABRICKS_TOKEN")

if not host:
    print("ERROR: DATABRICKS_HOST environment variable not set.")
    sys.exit(1)
if not token and not (os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET")):
    print("ERROR: DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET) not set.")
    sys.exit(1)

print(f"Connecting to: {host}")
print("Using serverless compute...")

spark = DatabricksSession.builder.serverless(True).getOrCreate()

row = spark.sql("SELECT 1 AS value").first()
assert row["value"] == 1, f"Expected 1, got {row['value']}"

print(f"[SMOKE] SELECT 1 serverless PASSED  (Spark {spark.version})")
spark.stop()
