"""Unit tests for _cloud.py's ambient (off-Databricks) GCS credential minting --
_mint_ambient_gcs_token() and its wiring into prepare_env().

Mocked google.auth only -- no network call, no real service-account key needed; gated on the
`gcp` extra (google-auth) being installed. Live-bucket validation (does a minted token
actually work end to end, inside a real Spark worker) is a separate concern -- see
test_cloud_gcs_live.py.
"""

import datetime
import os

import pytest

pytest.importorskip("google.auth")

from litebfx import _cloud    # noqa: E402


class _FakeCredentials:
    def __init__(self, token="fake-token", expiry=None):
        self.token = token
        self.expiry = expiry
        self.refresh_calls = 0

    def refresh(self, request):
        self.refresh_calls += 1


class _FakeGoogleAuth:
    """Stand-in for the `google.auth` module: only `.default(scopes=...)` is used."""

    def __init__(self, credentials=None, raise_on_default=None):
        self._credentials = credentials
        self._raise_on_default = raise_on_default
        self.default_calls = []

    def default(self, scopes=None):
        self.default_calls.append(scopes)
        if self._raise_on_default is not None:
            raise self._raise_on_default
        return self._credentials, "fake-project"


class _FakeTransportRequests:
    """Stand-in for `google.auth.transport.requests`: only `.Request()` is used, and only as
    an opaque object handed to credentials.refresh()."""

    def Request(self):
        return object()


@pytest.fixture(autouse=True)
def _reset_ambient_cache(monkeypatch):
    monkeypatch.setattr(_cloud, "_AMBIENT_GCS_CACHE", None)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("GCS_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)


def _install_fake_google_auth(monkeypatch, credentials=None, raise_on_default=None):
    fake_google_auth = _FakeGoogleAuth(credentials, raise_on_default)
    monkeypatch.setattr(_cloud, "_import_google_auth",
                         lambda: (fake_google_auth, _FakeTransportRequests()))
    return fake_google_auth


def test_no_op_when_google_application_credentials_unset(monkeypatch):
    # _import_google_auth is deliberately left unpatched (real google.auth) -- if the
    # function tried to import/call it despite the env var being unset, this would still
    # pass (real google.auth.default() would itself fail closed some other way), so the
    # real assertion is behavioral: no exception, clean None.
    assert _cloud._mint_ambient_gcs_token() is None


def test_mints_token_when_credentials_file_set(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    fake = _install_fake_google_auth(monkeypatch, _FakeCredentials(token="ya29.fake"))
    cred = _cloud._mint_ambient_gcs_token()
    assert cred is not None
    assert cred.env == {"GCS_OAUTH_TOKEN": "ya29.fake"}
    assert cred.fs_kwargs == {"access_token": "ya29.fake"}
    assert fake.default_calls == [["https://www.googleapis.com/auth/devstorage.read_only"]]


def test_expiry_converted_as_utc_not_local_time(monkeypatch):
    """credentials.expiry is a naive datetime that google-auth always sets in UTC --
    confirms the conversion doesn't fall through to interpreting it as local time, which
    would silently miscompute expiry on any host not already running UTC."""
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    naive_utc_expiry = datetime.datetime(2030, 1, 1, 0, 0, 0)
    _install_fake_google_auth(monkeypatch, _FakeCredentials(expiry=naive_utc_expiry))
    cred = _cloud._mint_ambient_gcs_token()
    expected_ms = datetime.datetime(2030, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc) \
        .timestamp() * 1000
    assert cred.expiration_time_ms == expected_ms


def test_no_expiry_means_never_expires(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    _install_fake_google_auth(monkeypatch, _FakeCredentials(expiry=None))
    cred = _cloud._mint_ambient_gcs_token()
    assert cred.expiration_time_ms is None
    assert not cred.is_expired()


def test_caches_and_does_not_remint_while_valid(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    fake = _install_fake_google_auth(monkeypatch, _FakeCredentials())
    first = _cloud._mint_ambient_gcs_token()
    second = _cloud._mint_ambient_gcs_token()
    assert first is second
    assert len(fake.default_calls) == 1


def test_remints_once_cached_credential_is_expired(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    expired = _cloud._VendedCredential({"GCS_OAUTH_TOKEN": "stale"}, {}, 0)
    monkeypatch.setattr(_cloud, "_AMBIENT_GCS_CACHE", expired)
    fake = _install_fake_google_auth(monkeypatch, _FakeCredentials(token="fresh"))
    cred = _cloud._mint_ambient_gcs_token()
    assert cred.env == {"GCS_OAUTH_TOKEN": "fresh"}
    assert len(fake.default_calls) == 1


def test_none_when_gcp_extra_not_installed(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    monkeypatch.setattr(_cloud, "_import_google_auth", lambda: None)
    assert _cloud._mint_ambient_gcs_token() is None


def test_none_on_auth_failure_does_not_raise(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    _install_fake_google_auth(monkeypatch, raise_on_default=RuntimeError("bad key file"))
    assert _cloud._mint_ambient_gcs_token() is None


def test_prepare_env_gcs_mints_off_databricks(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    _install_fake_google_auth(monkeypatch, _FakeCredentials(token="minted-token"))
    _cloud.prepare_env("gs://bucket/key")
    assert os.environ["GCS_OAUTH_TOKEN"] == "minted-token"


def test_prepare_env_gcs_noop_without_credentials_file(monkeypatch):
    before = dict(os.environ)
    _cloud.prepare_env("gs://bucket/key")
    assert dict(os.environ) == before


def test_prepare_env_s3_path_never_triggers_gcs_mint(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/fake/key.json")
    fake = _install_fake_google_auth(monkeypatch, _FakeCredentials())
    _cloud.prepare_env("s3://bucket/key")
    assert fake.default_calls == []
    assert "GCS_OAUTH_TOKEN" not in os.environ
