#!/usr/bin/env python3
"""Mints a GCS OAuth access token from GOOGLE_APPLICATION_CREDENTIALS and prints it to
stdout. Used by docker-compose's python-test-gcs-live / python-test-databricks-gcs-live
services to export GCS_OAUTH_TOKEN into the shell *before* pytest -- and the Spark JVM
gateway pytest launches -- starts.

Ordering matters here: pyspark.java_gateway.launch_gateway() snapshots os.environ once, at
Popen time, when the `spark` fixture first calls SparkSession.builder.getOrCreate(). A pytest
fixture that sets os.environ["GCS_OAUTH_TOKEN"] mid-test-run (e.g. after the spark fixture has
already launched the JVM) is too late -- the already-running JVM (and the Python worker
subprocesses it forks) never see it, and htslib's native GCS backend fails with a generic
"Permission denied" that looks like an IAM problem but isn't. Minting here, before the shell
exports it and pytest even starts, sidesteps the whole class of ordering bug -- this also
matches production usage more faithfully than a fixture-time mint would: litebfx's
_cloud.py prepare_env() deliberately does not mint GCS_OAUTH_TOKEN off Databricks (see its
module docstring), so a real user relies on exactly this kind of ambient, pre-set credential.
"""

import google.auth
import google.auth.transport.requests

credentials, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/devstorage.read_write"])
credentials.refresh(google.auth.transport.requests.Request())
print(credentials.token)
