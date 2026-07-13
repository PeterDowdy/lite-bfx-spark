"""gs:// orchestration-layer tests against fake-gcs-server.

Unlike S3 (HTS_S3_HOST/HTS_S3_ADDRESS_STYLE), htslib's native GCS backend has **no endpoint
override at all** -- confirmed by binary inspection of pysam's bundled htslib: only
GCS_OAUTH_TOKEN/GCS_REQUESTER_PAYS_PROJECT are read, and the compiled-in URL rewrite always
targets `.googleapis.com` (`hfile_gcs.c`'s `gcs_open`). A gs:// path can never be redirected
to fake-gcs-server at the htslib level, so a full pysam/Spark BAM-over-gs:// read against this
emulator is not achievable and is not attempted here (confirmed by direct testing: the read
hangs for 30+ minutes retrying real Google OAuth2 endpoints before failing).

What *is* testable against fake-gcs-server: the `_cloudfs.py` orchestration layer
(exists/getsize/stat/listdir), which goes through `pyarrow.fs.GcsFileSystem` constructed
explicitly with `endpoint_override` (see `GCS_ENDPOINT_URL` in `_cloudfs.py`) rather than
htslib. That's real, valuable coverage on its own -- it's the same dispatch code path used
for directory listing, index resolution, and unindexed-BAM byte-range splitting on `gs://`.

Full htslib-backed gs:// read correctness needs a real GCS bucket -- out of scope for this
emulator-based suite; see docs/proposals/python-data-source.md and TASKS.md.
"""

from litebfx import _cloudfs


def test_gcs_orchestration_exists_and_getsize(gcs_bucket):
    from conftest import resource
    import os

    path = f"{gcs_bucket}/range.bam"
    assert _cloudfs.exists(path)
    assert _cloudfs.getsize(path) == os.path.getsize(resource("range.bam"))
    assert not _cloudfs.exists(f"{gcs_bucket}/does-not-exist.bam")


def test_gcs_orchestration_listdir(gcs_bucket):
    names = _cloudfs.listdir(gcs_bucket)
    assert "range.bam" in names
    assert "range.bam.bai" in names
    assert "realn01.fa" in names


def test_gcs_orchestration_stat(gcs_bucket):
    size, mtime = _cloudfs.stat(f"{gcs_bucket}/range.bam")
    assert size > 0
    # fake-gcs-server does report an upload mtime, but real GCS behavior isn't guaranteed
    # identical -- only assert the type contract (metadata_value() must handle both).
    assert mtime is None or isinstance(mtime, float)
