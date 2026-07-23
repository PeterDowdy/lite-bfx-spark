"""Unit tests for _cloud.py's Databricks Unity Catalog AWS credential vending --
_DatabricksPathCredential, _databricks_notebook_context(), _vend_databricks_path_credential(),
databricks_credential_for() -- and their wiring into prepare_env(), cloud_read_scope(), and
_cloudfs.filesystem_for().

Mocked throughout -- no real Databricks workspace, no network call, and no real
pyarrow.fs.S3FileSystem/from_uri construction either (this dev/test container has no ambient
AWS credentials at all, and real construction risks IMDS/network credential-resolution
overhead that these tests have no need to pay just to confirm the right construction path was
picked with the right arguments).

Notably, open-source PySpark (what this container has) ships no `pyspark.dbutils` module at
all -- only real Databricks Runtime provides one -- so the DBUtils-fallback tests inject a
fake module into sys.modules rather than monkeypatching an attribute onto a module that
doesn't exist here. Whether any of this actually authenticates against a real Databricks
workspace -- the thing no mock can tell you -- is exactly what
tests/smoke_uc_credential_vending.py is for (manual, pre-release, not in default CI; see
TESTING.md).
"""

import json
import logging
import os
import sys
import time
import types
import urllib.error

import pytest

pyarrow_fs = pytest.importorskip("pyarrow.fs")

from litebfx import _cloud, _cloudfs, io    # noqa: E402


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("DATABRICKS_RUNTIME_VERSION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL", "GOOGLE_APPLICATION_CREDENTIALS",
                "GCS_OAUTH_TOKEN", "HTS_S3_HOST", "HTS_S3_ADDRESS_STYLE"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    monkeypatch.setattr(_cloud, "_DATABRICKS_CRED_CACHE", {})
    monkeypatch.setattr(_cloud, "_ACTIVE_CREDENTIAL", None)
    monkeypatch.setattr(_cloud, "_LAST_INJECTED_CREDENTIAL_KEYS", set())
    monkeypatch.setattr(_cloudfs, "_FS_CACHE", {})


@pytest.fixture(autouse=True)
def _no_real_region_lookup(monkeypatch):
    """pyarrow.fs.resolve_s3_region() -- the one real network call _resolve_s3_region() makes
    -- defaults to raising (same as "bucket not found"/no network), which _resolve_s3_region()
    itself catches and turns into a clean None, so every test that reaches
    _vend_databricks_path_credential()'s success path doesn't silently depend on network
    access. Patched at the pyarrow level, not _cloud._resolve_s3_region itself, so this
    function's own dedicated tests below still exercise its real try/except logic and can
    override this mock for their own scenarios."""
    monkeypatch.setattr(pyarrow_fs, "resolve_s3_region",
                         lambda bucket: (_ for _ in ()).throw(OSError("no network in tests")))


# --- _DatabricksPathCredential ------------------------------------------------------------

def test_repr_and_str_redact_secrets():
    cred = _cloud._DatabricksPathCredential("AKIAFAKE", "supersecret", "sessiontoken", 123456)
    r = repr(cred)
    assert "AKIAFAKE" not in r
    assert "supersecret" not in r
    assert "sessiontoken" not in r
    assert "123456" in r
    assert str(cred) == r


def test_env_includes_session_token_when_present():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    assert cred.env == {
        "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK", "AWS_SESSION_TOKEN": "TOK"}


def test_env_omits_session_token_when_absent():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    assert cred.env == {"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK"}


def test_fs_kwargs_includes_session_token_when_present():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    assert cred.fs_kwargs == {"access_key": "AK", "secret_key": "SK", "session_token": "TOK"}


def test_fs_kwargs_omits_session_token_when_absent():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    assert cred.fs_kwargs == {"access_key": "AK", "secret_key": "SK"}


def test_env_includes_region_when_present():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None, region="eu-west-1")
    assert cred.env["AWS_DEFAULT_REGION"] == "eu-west-1"


def test_env_omits_region_when_absent():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    assert "AWS_DEFAULT_REGION" not in cred.env


def test_fs_kwargs_includes_region_when_present():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None, region="eu-west-1")
    assert cred.fs_kwargs["region"] == "eu-west-1"


def test_fs_kwargs_omits_region_when_absent():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    assert "region" not in cred.fs_kwargs


def test_repr_shows_region_not_just_expiration():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None, region="eu-west-1")
    assert "eu-west-1" in repr(cred)


def test_no_expiration_never_expires():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    assert not cred.is_expired()


def test_is_expired_true_past_deadline():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", 0)
    assert cred.is_expired()


def test_is_expired_false_comfortably_before_deadline():
    future_ms = (time.time() + 3600) * 1000
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", future_ms)
    assert not cred.is_expired()


def test_is_expired_respects_skew_seconds():
    soon_ms = (time.time() + 30) * 1000    # expires in 30s
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", soon_ms)
    assert cred.is_expired()               # default 60s skew already treats this as expired
    assert not cred.is_expired(skew_seconds=0)


# --- is_databricks() -----------------------------------------------------------------------

def test_is_databricks_true_when_env_set(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    assert _cloud.is_databricks() is True


def test_is_databricks_false_when_env_unset():
    assert _cloud.is_databricks() is False


# --- _active_spark_session() ----------------------------------------------------------------

def test_active_spark_session_none_when_no_active_session(monkeypatch):
    from pyspark.sql import SparkSession
    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(lambda cls: None))
    assert _cloud._active_spark_session() is None


def test_active_spark_session_none_on_exception(monkeypatch):
    from pyspark.sql import SparkSession

    def boom(cls):
        raise RuntimeError("no JVM gateway")

    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(boom))
    assert _cloud._active_spark_session() is None


def test_active_spark_session_returns_active_session(monkeypatch):
    from pyspark.sql import SparkSession
    sentinel = object()
    monkeypatch.setattr(SparkSession, "getActiveSession", classmethod(lambda cls: sentinel))
    assert _cloud._active_spark_session() is sentinel


# --- _databricks_notebook_context() ---------------------------------------------------------

class _FakeConf:
    def __init__(self, workspace_url, raises=False):
        self._workspace_url = workspace_url
        self._raises = raises

    def get(self, key):
        if self._raises:
            raise Exception("no such conf key")
        return self._workspace_url


class _FakeSpark:
    def __init__(self, workspace_url="adb-123.4.azuredatabricks.net", raises=False):
        self.conf = _FakeConf(workspace_url, raises)


def _fake_dbutils(token):
    """A dbutils stand-in supporting exactly the chain _databricks_notebook_context() calls:
    .notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()."""
    return types.SimpleNamespace(notebook=types.SimpleNamespace(
        entry_point=types.SimpleNamespace(getDbutils=lambda: types.SimpleNamespace(
            notebook=lambda: types.SimpleNamespace(getContext=lambda: types.SimpleNamespace(
                apiToken=lambda: types.SimpleNamespace(get=lambda: token)))))))


def _install_fake_pyspark_dbutils(monkeypatch, dbutils_factory):
    """Inject a fake pyspark.dbutils module into sys.modules. Open-source PySpark (what this
    dev/test container has) ships no pyspark.dbutils module at all -- confirmed empirically,
    only real Databricks Runtime provides one -- so there's no existing attribute for
    monkeypatch.setattr to replace; this simulates what Databricks Runtime provides instead.
    dbutils_factory is called as dbutils_factory(spark), matching the real DBUtils(spark)
    constructor call in _databricks_notebook_context()."""
    fake_module = types.ModuleType("pyspark.dbutils")
    fake_module.DBUtils = dbutils_factory
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", fake_module)


def test_notebook_context_none_when_conf_get_raises():
    spark = _FakeSpark(raises=True)
    assert _cloud._databricks_notebook_context(spark) is None


def test_notebook_context_none_when_workspace_url_empty():
    spark = _FakeSpark(workspace_url="")
    assert _cloud._databricks_notebook_context(spark) is None


def test_notebook_context_none_when_dbutils_construction_fails(monkeypatch):
    def boom(spark):
        raise RuntimeError("no gateway")

    _install_fake_pyspark_dbutils(monkeypatch, boom)
    assert _cloud._databricks_notebook_context(_FakeSpark()) is None


def test_notebook_context_none_when_dbutils_module_missing_entirely(monkeypatch):
    # The natural state in this dev/test container: no mocking at all -- IPython isn't
    # installed either (confirmed empirically), so both resolution paths fail closed.
    assert _cloud._databricks_notebook_context(_FakeSpark()) is None


def test_notebook_context_none_when_token_chain_broken(monkeypatch):
    _install_fake_pyspark_dbutils(monkeypatch, lambda spark: object())    # no .notebook attr
    assert _cloud._databricks_notebook_context(_FakeSpark()) is None


def test_notebook_context_none_when_token_is_empty(monkeypatch):
    _install_fake_pyspark_dbutils(monkeypatch, lambda spark: _fake_dbutils(""))
    assert _cloud._databricks_notebook_context(_FakeSpark()) is None


def test_notebook_context_success_via_dbutils_fallback(monkeypatch):
    _install_fake_pyspark_dbutils(monkeypatch, lambda spark: _fake_dbutils("tok-xyz"))
    spark = _FakeSpark(workspace_url="adb-123.4.azuredatabricks.net")
    assert _cloud._databricks_notebook_context(spark) == (
        "adb-123.4.azuredatabricks.net", "tok-xyz")


# --- _resolve_s3_region() --------------------------------------------------------------------

def test_resolve_s3_region_returns_pyarrow_result(monkeypatch):
    monkeypatch.setattr(pyarrow_fs, "resolve_s3_region", lambda bucket: "eu-west-1")
    assert _cloud._resolve_s3_region("some-bucket") == "eu-west-1"


def test_resolve_s3_region_none_on_failure_does_not_raise(monkeypatch):
    def raise_lookup(bucket):
        raise OSError(f"Bucket {bucket!r} not found")

    monkeypatch.setattr(pyarrow_fs, "resolve_s3_region", raise_lookup)
    assert _cloud._resolve_s3_region("some-bucket") is None


def test_resolve_s3_region_logs_warning_on_failure(monkeypatch, caplog):
    monkeypatch.setattr(pyarrow_fs, "resolve_s3_region",
                         lambda bucket: (_ for _ in ()).throw(OSError("not found")))
    with caplog.at_level(logging.WARNING, logger="litebfx._cloud"):
        _cloud._resolve_s3_region("some-bucket")
    assert any("some-bucket" in r.getMessage() for r in caplog.records)


# --- _vend_databricks_path_credential() ------------------------------------------------------

class _FakeHttpResponse:
    def __init__(self, payload_bytes):
        self._payload_bytes = payload_bytes

    def read(self):
        return self._payload_bytes

    def close(self):
        pass    # urllib.error.HTTPError's own __del__ cleanup expects fp to support this

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def _install_fake_urlopen(monkeypatch, payload):
    captured = []

    def fake_urlopen(req, timeout=None):
        captured.append(req)
        return _FakeHttpResponse(json.dumps(payload).encode())

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    return captured


def _stub_driver_context(monkeypatch, workspace_url="adb-123.4.azuredatabricks.net", token="tok"):
    monkeypatch.setattr(_cloud, "_active_spark_session", lambda: object())
    monkeypatch.setattr(_cloud, "_databricks_notebook_context",
                         lambda spark: (workspace_url, token))


def test_vend_credential_none_when_no_active_spark_session(monkeypatch):
    monkeypatch.setattr(_cloud, "_active_spark_session", lambda: None)
    called = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: called.append(1))
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None
    assert called == []


def test_vend_credential_none_when_no_notebook_context(monkeypatch):
    monkeypatch.setattr(_cloud, "_active_spark_session", lambda: object())
    monkeypatch.setattr(_cloud, "_databricks_notebook_context", lambda spark: None)
    called = []
    monkeypatch.setattr("urllib.request.urlopen", lambda *a, **k: called.append(1))
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None
    assert called == []


def test_vend_credential_none_on_network_error(monkeypatch):
    _stub_driver_context(monkeypatch)

    def raise_urlopen(req, timeout=None):
        raise OSError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", raise_urlopen)
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None


def test_vend_credential_none_on_malformed_json_response(monkeypatch):
    _stub_driver_context(monkeypatch)

    def fake_urlopen(req, timeout=None):
        return _FakeHttpResponse(b"not json")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None


def test_vend_credential_logs_http_error_detail_and_returns_none(monkeypatch, caplog):
    """A 403 (or other non-2xx) on the *vending request itself* -- most commonly a missing
    EXTERNAL USE LOCATION grant -- must surface the actual status and response body in the
    driver log, not vanish into a bare `except Exception: return None`. This is the
    diagnostic this project's own history (see TASKS.md) needed multiple rounds to get right
    for the first, since-removed implementation -- log it from the start this time."""
    _stub_driver_context(monkeypatch)
    body = (b'{"error_code":"PERMISSION_DENIED","message":"User does not have EXTERNAL USE '
            b'LOCATION on External Location \'my-loc\'."}')
    err = urllib.error.HTTPError(
        url="https://adb-123.4.azuredatabricks.net/api/2.1/unity-catalog/"
            "temporary-path-credentials",
        code=403, msg="Forbidden", hdrs=None, fp=_FakeHttpResponse(body))

    def raise_http_error(req, timeout=None):
        raise err

    monkeypatch.setattr("urllib.request.urlopen", raise_http_error)
    with caplog.at_level(logging.WARNING, logger="litebfx._cloud"):
        result = _cloud._vend_databricks_path_credential("s3://bucket/key.bam")

    assert result is None
    assert len(caplog.records) == 1
    msg = caplog.records[0].getMessage()
    assert "403" in msg
    assert "PERMISSION_DENIED" in msg
    assert "EXTERNAL USE LOCATION" in msg


def test_vend_credential_logs_success_at_info_level(monkeypatch, caplog):
    _stub_driver_context(monkeypatch)
    _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK"},
        "expiration_time": 1234567890000,
    })
    with caplog.at_level(logging.INFO, logger="litebfx._cloud"):
        result = _cloud._vend_databricks_path_credential("s3://bucket/key.bam")

    assert result is not None
    assert any("vended" in r.getMessage().lower() for r in caplog.records)
    # never log the actual secret material, only ever the non-secret request/expiry details
    assert not any("SK" in r.getMessage() for r in caplog.records)


def test_vend_credential_none_when_aws_temp_credentials_missing(monkeypatch):
    _stub_driver_context(monkeypatch)
    _install_fake_urlopen(monkeypatch, payload={"expiration_time": 123})
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None


def test_vend_credential_none_when_secret_missing(monkeypatch):
    _stub_driver_context(monkeypatch)
    _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {"access_key_id": "AK"}})
    assert _cloud._vend_databricks_path_credential("s3://bucket/key.bam") is None


def test_vend_credential_session_token_optional_in_response(monkeypatch):
    _stub_driver_context(monkeypatch)
    _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK"}})
    cred = _cloud._vend_databricks_path_credential("s3://bucket/key.bam")
    assert cred.session_token is None
    assert "AWS_SESSION_TOKEN" not in cred.env


def test_vend_credential_success_parses_response_and_request_shape(monkeypatch):
    _stub_driver_context(monkeypatch, workspace_url="adb-123.4.azuredatabricks.net",
                         token="tok-abc")
    captured = _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {
            "access_key_id": "AKIAFAKE",
            "secret_access_key": "secretvalue",
            "session_token": "sessiontoken",
        },
        "expiration_time": 1234567890000,
    })

    cred = _cloud._vend_databricks_path_credential("s3://my-bucket/some/key.bam")

    assert cred.access_key_id == "AKIAFAKE"
    assert cred.secret_access_key == "secretvalue"
    assert cred.session_token == "sessiontoken"
    assert cred.expiration_time_ms == 1234567890000

    [req] = captured
    assert req.full_url == (
        "https://adb-123.4.azuredatabricks.net/api/2.1/unity-catalog/temporary-path-credentials")
    assert req.get_method() == "POST"
    assert req.get_header("Authorization") == "Bearer tok-abc"
    assert req.get_header("Content-type") == "application/json"
    assert json.loads(req.data) == {
        "url": "s3://my-bucket/some/key.bam", "operation": "PATH_READ"}


def test_vend_credential_resolves_and_attaches_region(monkeypatch):
    _stub_driver_context(monkeypatch)
    region_calls = []
    monkeypatch.setattr(_cloud, "_resolve_s3_region",
                         lambda bucket: region_calls.append(bucket) or "eu-west-1")
    _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK"}})

    cred = _cloud._vend_databricks_path_credential("s3://my-bucket/some/key.bam")

    assert cred.region == "eu-west-1"
    assert region_calls == ["my-bucket"]


def test_vend_credential_bare_bucket_path_has_no_trailing_slash(monkeypatch):
    _stub_driver_context(monkeypatch)
    captured = _install_fake_urlopen(monkeypatch, payload={
        "aws_temp_credentials": {"access_key_id": "AK", "secret_access_key": "SK"}})

    _cloud._vend_databricks_path_credential("s3://my-bucket")

    [req] = captured
    assert json.loads(req.data)["url"] == "s3://my-bucket"


# --- databricks_credential_for() -------------------------------------------------------------

def test_databricks_credential_for_none_when_not_on_databricks():
    assert _cloud.databricks_credential_for("s3://bucket/key.bam") is None


def test_databricks_credential_for_none_for_non_s3_scheme(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    assert _cloud.databricks_credential_for("gs://bucket/key.bam") is None
    assert _cloud.databricks_credential_for("/local/path.bam") is None


def test_databricks_credential_for_vends_and_caches_per_bucket(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    calls = []

    def fake_vend(path, operation="PATH_READ"):
        calls.append(path)
        return _cloud._DatabricksPathCredential("AK", "SK", None, None)   # never expires

    monkeypatch.setattr(_cloud, "_vend_databricks_path_credential", fake_vend)
    first = _cloud.databricks_credential_for("s3://bucket/key1.bam")
    second = _cloud.databricks_credential_for("s3://bucket/key2.bam")
    assert first is second
    assert len(calls) == 1


def test_databricks_credential_for_revends_once_expired(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    _cloud._DATABRICKS_CRED_CACHE[("s3", "bucket")] = \
        _cloud._DatabricksPathCredential("OLD", "OLD", None, 0)     # already expired
    calls = []

    def fake_vend(path, operation="PATH_READ"):
        calls.append(path)
        return _cloud._DatabricksPathCredential("NEW", "NEW", None, None)

    monkeypatch.setattr(_cloud, "_vend_databricks_path_credential", fake_vend)
    cred = _cloud.databricks_credential_for("s3://bucket/key.bam")
    assert cred.access_key_id == "NEW"
    assert len(calls) == 1


def test_databricks_credential_for_different_buckets_cached_independently(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    calls = []

    def fake_vend(path, operation="PATH_READ"):
        _, bucket, _ = io.bucket_key(path)
        calls.append(bucket)
        return _cloud._DatabricksPathCredential(bucket, bucket, None, None)

    monkeypatch.setattr(_cloud, "_vend_databricks_path_credential", fake_vend)
    a = _cloud.databricks_credential_for("s3://bucket-a/key.bam")
    b = _cloud.databricks_credential_for("s3://bucket-b/key.bam")
    assert (a.access_key_id, b.access_key_id) == ("bucket-a", "bucket-b")
    assert calls == ["bucket-a", "bucket-b"]


def test_databricks_credential_for_retries_after_failed_vend(monkeypatch):
    """A None result (vend failure) isn't sticky -- every subsequent call retries, since a
    transient failure shouldn't permanently poison the cache for a bucket that might succeed
    on the next partitions() call."""
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    calls = []

    def fake_vend(path, operation="PATH_READ"):
        calls.append(path)
        return None

    monkeypatch.setattr(_cloud, "_vend_databricks_path_credential", fake_vend)
    first = _cloud.databricks_credential_for("s3://bucket/key.bam")
    second = _cloud.databricks_credential_for("s3://bucket/key.bam")
    assert first is None and second is None
    assert len(calls) == 2


# --- prepare_env() with a driver-vended credential --------------------------------------------

def test_prepare_env_applies_given_credential():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    _cloud.prepare_env("s3://bucket/key.bam", credential=cred)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AK"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "SK"
    assert os.environ["AWS_SESSION_TOKEN"] == "TOK"


def test_prepare_env_no_credential_s3_path_untouched_beyond_endpoint_handling():
    before = dict(os.environ)
    _cloud.prepare_env("s3://bucket/key.bam")
    assert dict(os.environ) == before


def test_prepare_env_credential_skips_gcs_ambient_mint(monkeypatch):
    """Confirms prepare_env()'s own if/elif contract in isolation: a given credential always
    takes priority, regardless of scheme -- ambient GCS minting is only ever attempted in the
    no-credential branch. (In practice databricks_credential_for() already gates on S3-only
    schemes, so a real caller never actually hands prepare_env() a credential for a gs://
    path -- this test is about prepare_env()'s own contract, not that combination occurring.)
    """
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    mint_calls = []
    monkeypatch.setattr(_cloud, "_mint_ambient_gcs_token", lambda: mint_calls.append(1))
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    _cloud.prepare_env("gs://bucket/key.bam", credential=cred)
    assert mint_calls == []
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AK"


def test_prepare_env_credential_and_endpoint_override_compose(monkeypatch):
    """A Databricks credential and a non-AWS S3 endpoint (e.g. MinIO) aren't mutually
    exclusive at the prepare_env() level -- the endpoint-derived HTS_S3_HOST/ADDRESS_STYLE
    handling is unconditional, independent of the if/elif credential branch above it."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    _cloud.prepare_env("s3://bucket/key.bam", credential=cred)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AK"
    assert os.environ["HTS_S3_HOST"] == "minio:9000"


def test_prepare_env_clears_stale_credential_env_when_credential_becomes_none():
    """A worker process can be reused across partitions/files. If a later prepare_env() call
    gets no credential (vend failed, or a different, non-Databricks path), a credential
    injected by an EARLIER call must not silently linger in os.environ and get reused against
    a bucket it was never scoped for -- that's a real, confusing-403 hazard, not just
    untidiness."""
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    _cloud.prepare_env("s3://bucket-a/key.bam", credential=cred)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AK"

    _cloud.prepare_env("s3://bucket-b/key.bam", credential=None)
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ
    assert "AWS_SESSION_TOKEN" not in os.environ


def test_prepare_env_never_touches_env_vars_it_did_not_inject(monkeypatch):
    """The cleanup above must not clobber AWS credentials that were already ambient before
    litebfx ever touched the environment (e.g. a real instance-profile/manually-exported-key
    setup) -- only keys litebfx itself injected from a credential are fair game to clear."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AMBIENT_KEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AMBIENT_SECRET")
    _cloud.prepare_env("s3://bucket/key.bam", credential=None)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AMBIENT_KEY"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "AMBIENT_SECRET"


def test_prepare_env_swaps_stale_key_when_new_credential_lacks_session_token():
    cred1 = _cloud._DatabricksPathCredential("AK1", "SK1", "TOK1", None)
    _cloud.prepare_env("s3://bucket/key.bam", credential=cred1)
    assert os.environ["AWS_SESSION_TOKEN"] == "TOK1"

    cred2 = _cloud._DatabricksPathCredential("AK2", "SK2", None, None)   # no session token
    _cloud.prepare_env("s3://bucket/key.bam", credential=cred2)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AK2"
    assert "AWS_SESSION_TOKEN" not in os.environ


# --- cloud_read_scope() with a credential ------------------------------------------------------

def test_cloud_read_scope_sets_active_credential_for_native_path():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    assert _cloud.active_credential() is None
    with _cloud.cloud_read_scope("s3://bucket/key.bam", cred):
        assert _cloud.active_credential() is cred
    assert _cloud.active_credential() is None


def test_cloud_read_scope_local_path_never_activates_credential():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    with _cloud.cloud_read_scope("/local/path.bam", cred):
        assert _cloud.active_credential() is None


def test_cloud_read_scope_clears_credential_on_exception():
    cred = _cloud._DatabricksPathCredential("AK", "SK", None, None)
    with pytest.raises(RuntimeError):
        with _cloud.cloud_read_scope("s3://bucket/key.bam", cred):
            assert _cloud.active_credential() is cred
            raise RuntimeError("boom")
    assert _cloud.active_credential() is None


def test_cloud_read_scope_applies_credential_env_via_prepare_env():
    cred = _cloud._DatabricksPathCredential("AK", "SK", "TOK", None)
    with _cloud.cloud_read_scope("s3://bucket/key.bam", cred):
        assert os.environ["AWS_ACCESS_KEY_ID"] == "AK"
        assert os.environ["AWS_SESSION_TOKEN"] == "TOK"


# --- _cloudfs.filesystem_for() credential selection -------------------------------------------

class _FakeFileSystem:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _install_fake_pyarrow_fs(monkeypatch):
    """Replace pyarrow.fs.S3FileSystem/FileSystem.from_uri with recording fakes. Real
    construction in this offline, no-ambient-credentials container may attempt IMDS/network
    credential resolution -- these tests only need to confirm _cloudfs.py picked the right
    construction path with the right arguments, not exercise real pyarrow S3 auth."""
    s3_calls = []
    from_uri_calls = []

    def fake_s3(**kwargs):
        s3_calls.append(kwargs)
        return _FakeFileSystem(**kwargs)

    class _FakeFileSystemClass:
        # pyarrow.fs.FileSystem is a C-extension type -- its from_uri can't be patched in
        # place (setattr on the class itself raises "immutable type"), so the whole
        # FileSystem name is replaced at the pyarrow.fs module level instead; _cloudfs.py
        # looks it up fresh off that module on every call, so this is transparent to it.
        @staticmethod
        def from_uri(uri):
            from_uri_calls.append(uri)
            return _FakeFileSystem(), ""

    monkeypatch.setattr(pyarrow_fs, "S3FileSystem", fake_s3)
    monkeypatch.setattr(pyarrow_fs, "FileSystem", _FakeFileSystemClass)
    return s3_calls, from_uri_calls


def test_filesystem_for_uses_active_credential(monkeypatch):
    s3_calls, from_uri_calls = _install_fake_pyarrow_fs(monkeypatch)
    cred = _cloud._DatabricksPathCredential("AK1", "SK1", "TOK1", None)
    monkeypatch.setattr(_cloud, "active_credential", lambda: cred)
    fallback_calls = []
    monkeypatch.setattr(_cloud, "databricks_credential_for",
                         lambda p: fallback_calls.append(p) or None)

    fs, within = _cloudfs.filesystem_for("s3://bucket/some/key.bam")

    assert within == "bucket/some/key.bam"
    assert fallback_calls == []
    assert s3_calls == [{"access_key": "AK1", "secret_key": "SK1", "session_token": "TOK1"}]
    assert from_uri_calls == []


def test_filesystem_for_falls_back_to_databricks_credential_for(monkeypatch):
    s3_calls, from_uri_calls = _install_fake_pyarrow_fs(monkeypatch)
    monkeypatch.setattr(_cloud, "active_credential", lambda: None)
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    cred = _cloud._DatabricksPathCredential("AK2", "SK2", None, None)
    monkeypatch.setattr(_cloud, "databricks_credential_for", lambda p: cred)

    _cloudfs.filesystem_for("s3://bucket/key.bam")

    assert s3_calls == [{"access_key": "AK2", "secret_key": "SK2"}]
    assert from_uri_calls == []


def test_filesystem_for_skips_databricks_fallback_off_databricks(monkeypatch):
    s3_calls, from_uri_calls = _install_fake_pyarrow_fs(monkeypatch)
    monkeypatch.setattr(_cloud, "active_credential", lambda: None)
    monkeypatch.setattr(_cloud, "is_databricks", lambda: False)
    called = []
    monkeypatch.setattr(_cloud, "databricks_credential_for",
                         lambda p: called.append(p) or None)

    _cloudfs.filesystem_for("s3://bucket/key.bam")

    assert called == []
    assert s3_calls == []
    assert from_uri_calls == ["s3://bucket"]      # ambient path: ordinary from_uri construction


def test_filesystem_for_credential_refresh_invalidates_old_cache_entry(monkeypatch):
    _install_fake_pyarrow_fs(monkeypatch)
    cred1 = _cloud._DatabricksPathCredential("AK1", "SK1", None, None)
    monkeypatch.setattr(_cloud, "active_credential", lambda: cred1)
    fs1, _ = _cloudfs.filesystem_for("s3://bucket/key.bam")
    assert len(_cloudfs._FS_CACHE) == 1

    cred2 = _cloud._DatabricksPathCredential("AK2", "SK2", None, None)
    monkeypatch.setattr(_cloud, "active_credential", lambda: cred2)
    fs2, _ = _cloudfs.filesystem_for("s3://bucket/key.bam")

    assert len(_cloudfs._FS_CACHE) == 1     # stale entry evicted, not accumulated
    assert fs1 is not fs2


def test_filesystem_for_gcs_path_never_consults_credentials(monkeypatch):
    """GCS isn't part of this feature (AWS/S3 only -- see _cloud.py's module docstring):
    confirms a gs:// path never even asks active_credential()/databricks_credential_for()."""
    _install_fake_pyarrow_fs(monkeypatch)
    monkeypatch.setattr(_cloud, "is_databricks", lambda: True)
    active_calls, db_calls = [], []
    monkeypatch.setattr(_cloud, "active_credential", lambda: active_calls.append(1) or None)
    monkeypatch.setattr(_cloud, "databricks_credential_for",
                         lambda p: db_calls.append(p) or None)

    _cloudfs.filesystem_for("gs://bucket/key.bam")

    assert active_calls == []
    assert db_calls == []
