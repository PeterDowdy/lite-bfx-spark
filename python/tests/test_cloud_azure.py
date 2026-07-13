"""Direct abfss:// reads against Azurite -- download-then-open via pyarrow.fs.AzureFileSystem
(htslib has no native Azure backend at all, confirmed by binary inspection; see io.py).

Unlike GCS, full pysam/Spark-level reads *are* testable against the Azurite emulator: the
whole point of the download fallback is that pysam only ever sees a real local file, so
there's no htslib-level endpoint restriction to work around here. Skipped entirely (not
failed) unless AZURE_STORAGE_ENDPOINT_URL is set -- run via `docker compose run --rm
python-test-azure`, not in default CI.
"""

import pytest

from litebfx import _cloudfs

pytestmark = pytest.mark.spark


def test_azure_full_scan_matches_local(spark, azure_container):
    import pysam
    from conftest import resource
    got = spark.read.format("bam").option("indexPath", f"{azure_container}/range.bam.bai") \
        .load(f"{azure_container}/range.bam").count()
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch(until_eof=True))
    assert got == exp > 0


def test_azure_region_query_matches_local(spark, azure_container):
    import pysam
    from conftest import resource
    got = (spark.read.format("bam")
           .option("indexPath", f"{azure_container}/range.bam.bai")
           .option("region", "CHROMOSOME_I:1-2000")
           .load(f"{azure_container}/range.bam").count())
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch("CHROMOSOME_I", 0, 2000))
    assert got == exp > 0


def test_azure_materialize_local_caches_by_source_path(azure_container):
    """A second materialize_local() call for the same source path must return the cached
    local copy, not re-download -- the whole point of the cache."""
    path = f"{azure_container}/range.bam"
    first = _cloudfs.materialize_local(path)
    second = _cloudfs.materialize_local(path)
    assert first == second
    import os
    assert os.path.exists(first)
