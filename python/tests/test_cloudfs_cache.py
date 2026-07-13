"""Unit tests for _cloudfs.py's filesystem cache -- specifically that a credential refresh
(or a retry after a transient vending failure) invalidates the cached pyarrow.fs.FileSystem
instead of it being reused forever. See filesystem_for()'s docstring for the bug this covers:
a single transient vend_credential() failure on the first cloud touch in a worker process
used to permanently pin that bucket to the unauthorized ambient-fallback filesystem, even
though _cloud.credentials_for() itself retries correctly on every call.

Fully mocked: no real pyarrow.fs.S3FileSystem/AzureFileSystem construction, no network, no
Databricks SDK needed.
"""

import pytest

from litebfx import _cloud, _cloudfs


class _FakeFs:
    """Stand-in for a pyarrow.fs.FileSystem -- only identity and constructor kwargs matter
    for these tests, not real filesystem behavior."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FakeFileSystem:
    """Stand-in for pyarrow.fs.FileSystem itself (only its from_uri staticmethod is used by
    filesystem_for()'s ambient fallback path). pyarrow._fs.FileSystem is a Cython extension
    type -- its attributes can't be monkeypatched directly ("immutable type") -- but
    pyarrow.fs.FileSystem is just a name binding in the pure-Python wrapper module's
    namespace, so replacing *that* (module-attribute reassignment, not extension-type
    mutation) works fine and doesn't touch the real extension type at all."""

    @staticmethod
    def from_uri(uri):
        return _FakeFs(from_uri=uri), None


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    monkeypatch.setattr(_cloudfs, "_FS_CACHE", {})
    monkeypatch.setattr(_cloudfs, "_FS_CTOR", {
        "s3": _FakeFs, "s3a": _FakeFs, "s3n": _FakeFs, "gs": _FakeFs, "gcs": _FakeFs,
    })
    monkeypatch.setattr(_cloudfs.pyarrow.fs, "FileSystem", _FakeFileSystem)
    monkeypatch.delenv("GCS_ENDPOINT_URL", raising=False)


def test_reuses_cached_filesystem_when_credential_unchanged(monkeypatch):
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    cred = _cloud._VendedCredential({}, {"access_key": "AKIA"}, None)
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: cred)
    fs1, _ = _cloudfs.filesystem_for("s3://bucket/key1")
    fs2, _ = _cloudfs.filesystem_for("s3://bucket/key2")
    assert fs1 is fs2


def test_rebuilds_filesystem_when_credential_refreshes(monkeypatch):
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    creds = iter([_cloud._VendedCredential({}, {"access_key": "old"}, None),
                  _cloud._VendedCredential({}, {"access_key": "new"}, None)])
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: next(creds))
    fs1, _ = _cloudfs.filesystem_for("s3://bucket/key")
    fs2, _ = _cloudfs.filesystem_for("s3://bucket/key")
    assert fs1 is not fs2
    assert fs1.kwargs == {"access_key": "old"}
    assert fs2.kwargs == {"access_key": "new"}


def test_stale_entry_evicted_not_just_shadowed(monkeypatch):
    """Confirms _cache_fs() deletes the old entry rather than leaving it as dead weight
    alongside the new one -- the memory-growth concern noted in filesystem_for()'s
    docstring, for a worker process that lives long enough to see several credential
    refreshes against the same bucket."""
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    creds = iter([_cloud._VendedCredential({}, {"access_key": "old"}, None),
                  _cloud._VendedCredential({}, {"access_key": "new"}, None)])
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: next(creds))
    _cloudfs.filesystem_for("s3://bucket/key")
    _cloudfs.filesystem_for("s3://bucket/key")
    matching = [k for k in _cloudfs._FS_CACHE if k[:2] == ("s3", "bucket")]
    assert len(matching) == 1


def test_recovers_after_transient_vending_failure(monkeypatch):
    """The exact bug this fix closes: vend_credential() failing (returning None) on the
    *first* cloud touch in a worker process must not permanently pin the bucket to the
    unauthorized ambient-fallback filesystem. _cloud.credentials_for() itself already retries
    vending on every call while its cache holds None (confirmed by reading its source) --
    filesystem_for() must re-check it every time too, not cache the ambient fallback forever
    just because that's what the first call happened to produce."""
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    creds = iter([None, _cloud._VendedCredential({}, {"access_key": "vended"}, None)])
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: next(creds))
    fs1, _ = _cloudfs.filesystem_for("s3://bucket/key")   # vending "failed" -> ambient fallback
    assert fs1.kwargs == {"from_uri": "s3://bucket"}
    fs2, _ = _cloudfs.filesystem_for("s3://bucket/key")   # vending recovers on retry
    assert fs2.kwargs == {"access_key": "vended"}
    assert fs1 is not fs2


def test_different_buckets_cached_independently(monkeypatch):
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    cred = _cloud._VendedCredential({}, {"access_key": "AKIA"}, None)
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: cred)
    fs_a, _ = _cloudfs.filesystem_for("s3://bucket-a/key")
    fs_b, _ = _cloudfs.filesystem_for("s3://bucket-b/key")
    assert fs_a is not fs_b
    assert len(_cloudfs._FS_CACHE) == 2


def test_off_databricks_uses_ambient_and_still_caches(monkeypatch):
    monkeypatch.setattr(_cloud, "is_databricks", lambda: False)
    called = []
    monkeypatch.setattr(_cloud, "credentials_for", lambda path: called.append(path) or None)
    fs1, _ = _cloudfs.filesystem_for("s3://bucket/key1")
    fs2, _ = _cloudfs.filesystem_for("s3://bucket/key2")
    assert fs1 is fs2
    assert called == []    # credentials_for() never called when not on Databricks
