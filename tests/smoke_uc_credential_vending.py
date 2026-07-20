# Databricks Unity Catalog path-credential vending smoke test (take two): confirms
# _cloud.py's driver-vended AWS credential path authenticates against a real workspace.
"""

Unlike smoke_serverless.py (and the pre-removal version of this script -- see
python/TASKS.md's "Re-implemented: Unity Catalog credential vending, take two" entry), this
CANNOT run via databricks-connect (Spark Connect) from a local/CI machine. The whole point of
the redesign is that credential vending happens driver-side via dbutils's own notebook-context
API token (_cloud._databricks_notebook_context()), and dbutils -- in the specific shape this
code depends on -- is only reliably present on a driver process Databricks itself launched
(a real notebook, or a Job's Python task), not one connecting remotely through Spark Connect's
gateway-less client. Running this via databricks-connect would just silently fall back to "no
credential" / ambient resolution and confirm nothing about the actual mechanism being tested.

Prerequisites:
  - Run as a cell in an actual Databricks notebook attached to a cluster or serverless
    compute, OR as a Databricks Job "Python script" task pointing at this file -- either way,
    `spark` and `dbutils` must already exist as real driver-side globals, which only holds for
    a process Databricks itself launched.
  - litebfx installed in that environment (a notebook-scoped `%pip install lite-bfx-spark`
    cell above this one, or a Job library).
  - The identity running the notebook/job needs EXTERNAL USE LOCATION on the external
    location backing UC_TEST_BAM_URL below -- a different, narrower grant than the removed
    first implementation's READ FILES; see _cloud.py's module docstring.
  - UC_TEST_BAM_URL: a direct s3:// URL to a small BAM under that external location (AWS
    only -- this mechanism doesn't cover gs:///abfss://, see _cloud.py). Set it as a plain
    env var (works for a Job's Python task) or a notebook widget (dbutils.widgets.get) --
    this script checks the env var first, then falls back to a widget. A co-located or
    indexPath-resolvable .bai is optional but recommended: it exercises the region-query
    path too, not just a full scan.

Run this manually before a release that touches _cloud.py, _cloudfs.py, or any format
reader's partitions()/credential wiring -- it is not part of default CI (no real workspace
there) and has not yet been re-run against a live workspace since this reimplementation.
"""

import os
import sys

bam_url = os.environ.get("UC_TEST_BAM_URL")
if not bam_url:
    try:
        bam_url = dbutils.widgets.get("UC_TEST_BAM_URL")    # noqa: F821 -- notebook/job global
    except Exception:
        bam_url = None
if not bam_url:
    print("ERROR: UC_TEST_BAM_URL not set (env var or notebook widget) -- point it at a "
          "small BAM under a UC External Location this identity has EXTERNAL USE LOCATION "
          "on. s3:// only -- this mechanism is AWS-only for now, see _cloud.py.")
    sys.exit(1)
if not bam_url.startswith("s3://"):
    print(f"ERROR: UC_TEST_BAM_URL must be an s3:// URL (got {bam_url!r}) -- Databricks path-"
          "credential vending is AWS-only in this implementation, see _cloud.py.")
    sys.exit(1)

print(f"Reading (full scan): {bam_url}")

import litebfx    # noqa: E402
litebfx.register_all(spark)    # noqa: F821 -- notebook/job global

count = spark.read.format("bam").load(bam_url).count()    # noqa: F821 -- notebook/job global
assert count > 0, f"Expected at least one read, got {count} -- empty file, or the read " \
    "silently returned nothing instead of raising (check the driver log for a swallowed error)."

print(f"[SMOKE] Databricks UC path-credential vending PASSED  ({count} reads)")
print("This confirms the driver vended a Unity Catalog path credential via dbutils's "
      "notebook-context API token and threaded it to the worker that performed this read -- "
      "see litebfx/_cloud.py's module docstring.")
