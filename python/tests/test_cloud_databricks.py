"""Unit tests for _cloud.py's Databricks/Unity Catalog credential-vending logic.

Mocked responses only -- constructed from the real databricks-sdk dataclasses so a future SDK
response-shape change breaks this test rather than silently mismatching _cloud.py's parsing.
No network call, no real workspace needed; gated on the `databricks` extra being installed.
Real-workspace validation (does WorkspaceClient() auth actually resolve inside an isolated
Python Data Source worker subprocess?) is a separate, unverifiable-in-CI concern -- see
tests/smoke_uc_credential_vending.py.
"""

import os

import pytest

pytest.importorskip("databricks.sdk")

from databricks.sdk.service.catalog import (       # noqa: E402
    AwsCredentials, AzureActiveDirectoryToken, AzureUserDelegationSas,
    GcpOauthToken, GenerateTemporaryPathCredentialResponse,
)

from litebfx import _cloud                          # noqa: E402


def test_is_databricks(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert not _cloud.is_databricks()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "17.3")
    assert _cloud.is_databricks()


def test_credential_from_response_aws_full():
    resp = GenerateTemporaryPathCredentialResponse(
        aws_temp_credentials=AwsCredentials(
            access_key_id="AKIA...", secret_access_key="secret", session_token="token"),
        expiration_time=1234567890000)
    cred = _cloud._credential_from_response(resp)
    assert cred.env == {"AWS_ACCESS_KEY_ID": "AKIA...", "AWS_SECRET_ACCESS_KEY": "secret",
                         "AWS_SESSION_TOKEN": "token"}
    assert cred.fs_kwargs == {"access_key": "AKIA...", "secret_key": "secret",
                               "session_token": "token"}
    assert cred.expiration_time_ms == 1234567890000


def test_credential_from_response_aws_no_session_token():
    """Permanent-style credentials (no session token) shouldn't emit a null token entry --
    htslib/pyarrow.fs should not see AWS_SESSION_TOKEN="None"."""
    resp = GenerateTemporaryPathCredentialResponse(
        aws_temp_credentials=AwsCredentials(access_key_id="AKIA...", secret_access_key="s"))
    cred = _cloud._credential_from_response(resp)
    assert "AWS_SESSION_TOKEN" not in cred.env
    assert "session_token" not in cred.fs_kwargs


def test_credential_from_response_gcp():
    resp = GenerateTemporaryPathCredentialResponse(
        gcp_oauth_token=GcpOauthToken(oauth_token="ya29...."), expiration_time=999)
    cred = _cloud._credential_from_response(resp)
    assert cred.env == {"GCS_OAUTH_TOKEN": "ya29...."}
    assert cred.fs_kwargs["access_token"] == "ya29...."
    assert cred.fs_kwargs["credential_token_expiration"].timestamp() == pytest.approx(0.999)
    assert cred.expiration_time_ms == 999


def test_credential_from_response_gcp_no_expiration():
    """No expiration_time -> no credential_token_expiration kwarg (would otherwise crash
    GcsFileSystem's constructor, which requires it whenever access_token is set)."""
    resp = GenerateTemporaryPathCredentialResponse(
        gcp_oauth_token=GcpOauthToken(oauth_token="ya29...."))
    cred = _cloud._credential_from_response(resp)
    assert "credential_token_expiration" not in cred.fs_kwargs


def test_credential_from_response_azure_aad():
    """Confirmed gap: no pyarrow.fs.AzureFileSystem kwarg accepts a pre-obtained bearer
    token, so this maps to nothing usable -- see _cloud.py's comment on this branch."""
    resp = GenerateTemporaryPathCredentialResponse(
        azure_aad=AzureActiveDirectoryToken(aad_token="eyJ..."))
    cred = _cloud._credential_from_response(resp)
    assert cred.env == {}
    assert cred.fs_kwargs == {}


def test_credential_from_response_azure_sas():
    resp = GenerateTemporaryPathCredentialResponse(
        azure_user_delegation_sas=AzureUserDelegationSas(sas_token="sv=..."))
    cred = _cloud._credential_from_response(resp)
    assert cred.env == {}    # Azure never reaches htslib -- no env vars, ever
    assert cred.fs_kwargs == {"sas_token": "sv=..."}


def test_credential_from_response_unrecognized_shape():
    resp = GenerateTemporaryPathCredentialResponse()   # no credential field populated at all
    assert _cloud._credential_from_response(resp) is None


def test_vended_credential_is_expired(monkeypatch):
    import time
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(time, "time", lambda: now_ms / 1000)
    fresh = _cloud._VendedCredential({}, {}, now_ms + 3_600_000)   # 1h out
    about_to_expire = _cloud._VendedCredential({}, {}, now_ms + 30_000)   # 30s out
    no_expiry = _cloud._VendedCredential({}, {}, None)
    assert not fresh.is_expired()
    assert about_to_expire.is_expired(skew_seconds=60)   # inside the 60s skew window
    assert not no_expiry.is_expired()


def test_vend_credential_off_databricks_no_network_call(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert _cloud.vend_credential("s3://bucket/key") is None


def test_prepare_env_off_databricks_is_noop(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    before = dict(os.environ)
    _cloud.prepare_env("s3://bucket/key")
    assert dict(os.environ) == before
