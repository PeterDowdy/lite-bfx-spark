"""Cloud credentials for htslib's native S3/GCS remote-read backend, and for the pyarrow.fs
filesystems _cloudfs.py uses for orchestration (including Azure's download-fallback path).

For S3 and Azure, ambient credentials (env vars, instance metadata, shared config) need
nothing from this module -- htslib and pyarrow.fs both resolve them on their own. GCS is a
real exception: htslib's native GCS backend wants an already-minted OAuth *access token* in
GCS_OAUTH_TOKEN, not GOOGLE_APPLICATION_CREDENTIALS (a key *file path*) -- the credential
shape most GCS users actually have ambiently, and the one pyarrow.fs.GcsFileSystem itself
resolves happily on its own. Left alone, that's a silent read failure for anyone off
Databricks with only a service-account key file. prepare_env() closes this: when
GOOGLE_APPLICATION_CREDENTIALS is set and the `gcp` extra (google-auth) is installed, it
mints an access token from the key file directly (see _mint_ambient_gcs_token()) -- cached
process-wide and refreshed near expiry, independent of Databricks. Missing extra, missing env
var, or any minting failure is a silent no-op, same as the rest of this module's failure
posture: htslib/pyarrow.fs just fall back to whatever's already ambient.

On Databricks, prepare_env() *additionally* vends a short-lived, path-scoped credential via
Unity Catalog's Temporary Credentials API and writes it into os.environ, taking priority over
both plain ambient resolution and the GOOGLE_APPLICATION_CREDENTIALS mint above (it's scoped
to exactly what the caller is authorized for). Vending is lazy -- the first cloud open in a
worker process -- and cached per (scheme, bucket), re-vending near expiry. Off Databricks, or
if databricks-sdk isn't importable, or if vending fails for any reason, this step is skipped
and prepare_env() falls through to the GOOGLE_APPLICATION_CREDENTIALS mint (GCS) or plain
ambient resolution (S3/Azure). databricks-sdk is deliberately not a litebfx extra -- it's
preinstalled on every Databricks Runtime/Serverless image, and declaring it here forces pip
to reconcile against whatever's already pinned there instead of just using it; see
pyproject.toml's comment on this for the real install failure that caused the change.

Not threaded through InputPartition: InputPartition objects get logged in Spark's plan
explain output and worker tracebacks (a credential-bearing field is a real exposure risk),
and htslib reads credentials from os.environ, not a constructor argument, so a dataclass
field would just be an indirect carrier for something that has to land in global env state
right before use anyway. Nothing here ever crosses the driver->worker cloudpickle boundary.

Concurrency: os.environ mutation is process-global, and it's unverified whether htslib
re-reads env vars only at open time or on every subsequent range request against an
already-open handle. cloud_read_lock() must be held for the *entire duration* of a
cloud-backed partition's read -- from prepare_env() through exhausting that partition's read
generator, not just around the open call -- so cloud-backed reads are serialized within one
worker process regardless of worker threading model or htslib's actual env-read timing.
Local-path reads never touch this lock.
"""

import contextlib
import logging
import os
import threading
import time
import urllib.parse

from . import io as _io

_logger = logging.getLogger(__name__)

# htslib's S3 backend recognizes only s3://, s3+http://, and s3+https:// -- s3a:// and
# s3n:// give "Protocol not supported" (confirmed empirically), so they're normalized to
# s3(+scheme):// before ever reaching pysam. The URL's *authority* position is the bucket,
# never a host -- htslib has no way to point at a non-default host except HTS_S3_HOST, and
# plain s3:// always means real AWS regardless of HTS_S3_HOST (confirmed empirically against
# MinIO: s3://bucket/key with only HTS_S3_HOST set still fails "No such file or directory" --
# the +http/+https scheme suffix is what actually switches the target host, not just the env
# var). effective_path() below handles all of this.
_S3_SCHEMES = ("s3", "s3a", "s3n")
_GCS_SCHEMES = ("gs", "gcs")

_LOCK = threading.RLock()

_CACHE = {}   # (scheme, bucket) -> _VendedCredential | None

# Ambient GCS token minted from GOOGLE_APPLICATION_CREDENTIALS -- unlike _CACHE above, this
# is a single process-wide credential, not per-bucket: one service-account key file mints one
# token usable against every bucket that key is authorized for, so there is no bucket key to
# scope it by. None means "not yet minted"; a _VendedCredential that is_expired() means
# "mint again". See _mint_ambient_gcs_token().
_AMBIENT_GCS_CACHE = None


def cloud_read_lock():
    """The lock callers must hold for the full duration of a cloud-backed partition's read --
    see module docstring."""
    return _LOCK


@contextlib.contextmanager
def cloud_read_scope(path: str):
    """prepare_env(path) + hold cloud_read_lock(), for a "native" cloud path; a no-op (no
    lock, no env prep) for a local path. Wrap a format reader's entire open-through-
    generator-exhaustion body in this -- it's a generator-safe context manager (the lock
    stays held across `yield`s, released only when the wrapped generator is exhausted or
    closed), which is what "entire duration of a partition's read" requires. Azure
    (download mode) doesn't need this: it never touches os.environ.

    This only sets up the *environment* -- callers must still pass every path they open
    (the main file AND any index) through effective_path() before handing it to pysam; see
    that function's docstring for why the path itself, not just env vars, can need rewriting.
    """
    if _io.cloud_read_mode(path) == "native":
        with _LOCK:
            prepare_env(path)
            yield
    else:
        yield


def is_databricks() -> bool:
    """True when running inside a Databricks cluster/serverless process, driver or worker --
    DATABRICKS_RUNTIME_VERSION is set on every node."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _import_databricks_sdk():
    """Lazy import, same pattern as _base.import_pysam() -- returns None (not a raise) when
    databricks-sdk isn't importable; caller falls back to ambient credentials. Deliberately
    not a litebfx extra -- see pyproject.toml's comment on this -- so "not installed" here
    almost always means "genuinely off Databricks", not "forgot to pip install an extra"."""
    try:
        import databricks.sdk
        return databricks.sdk
    except ImportError:
        return None


def _import_google_auth():
    """Lazy import, same pattern as _import_databricks_sdk() -- returns None (not a raise)
    when the `gcp` extra isn't installed; caller falls back to whatever's already ambient."""
    try:
        import google.auth
        import google.auth.transport.requests
        return google.auth, google.auth.transport.requests
    except ImportError:
        return None


class _VendedCredential:
    __slots__ = ("env", "fs_kwargs", "expiration_time_ms")

    def __init__(self, env, fs_kwargs, expiration_time_ms):
        self.env = env                            # for htslib (S3/GCS only -- Azure never
        self.fs_kwargs = fs_kwargs                 # reaches htslib) and for pyarrow.fs
        self.expiration_time_ms = expiration_time_ms

    def is_expired(self, skew_seconds=60) -> bool:
        if self.expiration_time_ms is None:
            return False
        return time.time() * 1000 >= self.expiration_time_ms - skew_seconds * 1000


def _mint_ambient_gcs_token():
    """Mint (or reuse a still-valid cached) OAuth access token from
    GOOGLE_APPLICATION_CREDENTIALS -- the off-Databricks counterpart to vend_credential()'s
    Unity Catalog path: same _VendedCredential shape and cache-and-refresh-near-expiry
    behavior, but sourced from a static service-account key file via google-auth instead of a
    per-call vending API. Read-only scope -- this package never writes to cloud storage.

    Returns None on ANY failure (GOOGLE_APPLICATION_CREDENTIALS unset, `gcp` extra not
    installed, malformed key file, network) -- every failure mode falls back to whatever's
    already ambient, never raises. Cached process-wide in _AMBIENT_GCS_CACHE: one
    service-account key mints one token usable against any bucket that key can reach, so
    there's no per-bucket cache key the way Databricks' path-scoped vended credentials need.
    """
    global _AMBIENT_GCS_CACHE
    if _AMBIENT_GCS_CACHE is not None and not _AMBIENT_GCS_CACHE.is_expired():
        return _AMBIENT_GCS_CACHE
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return None
    mods = _import_google_auth()
    if mods is None:
        return None
    google_auth, google_auth_transport_requests = mods
    try:
        credentials, _ = google_auth.default(
            scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
        credentials.refresh(google_auth_transport_requests.Request())
        expiration_time_ms = None
        if credentials.expiry is not None:
            # credentials.expiry is a naive datetime that google-auth always sets in UTC
            # (confirmed: every credential type's refresh() populates it via
            # datetime.utcnow() + expires_in) -- .timestamp() on a naive datetime instead
            # interprets it in the *local* system timezone, which would silently miscompute
            # expiry on any host not already running UTC.
            import datetime
            expiration_time_ms = credentials.expiry.replace(
                tzinfo=datetime.timezone.utc).timestamp() * 1000
        cred = _VendedCredential(
            {"GCS_OAUTH_TOKEN": credentials.token}, {"access_token": credentials.token},
            expiration_time_ms)
        _AMBIENT_GCS_CACHE = cred
        return cred
    except Exception:
        return None


def _credential_from_response(resp):
    """Map a GenerateTemporaryPathCredentialResponse to a _VendedCredential, or None for a
    cloud/auth shape this version doesn't recognize. fs_kwargs are confirmed against the
    actual pyarrow.fs.{S3,Gcs,Azure}FileSystem constructor signatures, not guessed."""
    if resp.aws_temp_credentials is not None:
        c = resp.aws_temp_credentials
        env = {"AWS_ACCESS_KEY_ID": c.access_key_id, "AWS_SECRET_ACCESS_KEY": c.secret_access_key}
        fs_kwargs = {"access_key": c.access_key_id, "secret_key": c.secret_access_key}
        if c.session_token:
            env["AWS_SESSION_TOKEN"] = c.session_token
            fs_kwargs["session_token"] = c.session_token
        return _VendedCredential(env, fs_kwargs, resp.expiration_time)
    if resp.gcp_oauth_token is not None:
        token = resp.gcp_oauth_token.oauth_token
        env = {"GCS_OAUTH_TOKEN": token}
        # GcsFileSystem(access_token=..., credential_token_expiration=...) accepts a
        # pre-obtained token directly (confirmed against its constructor signature) --
        # credential_token_expiration wants a datetime, not the epoch-ms this API returns.
        fs_kwargs = {"access_token": token}
        if resp.expiration_time is not None:
            import datetime
            fs_kwargs["credential_token_expiration"] = datetime.datetime.fromtimestamp(
                resp.expiration_time / 1000, tz=datetime.timezone.utc)
        return _VendedCredential(env, fs_kwargs, resp.expiration_time)
    if resp.azure_user_delegation_sas is not None:
        # AzureFileSystem(sas_token=...) accepts this directly (confirmed against its
        # constructor signature). Azure never reaches htslib -- no env vars, ever.
        return _VendedCredential({}, {"sas_token": resp.azure_user_delegation_sas.sas_token},
                                  resp.expiration_time)
    if resp.azure_aad is not None:
        # Confirmed gap, not a guess: pyarrow.fs.AzureFileSystem's constructor has no kwarg
        # for injecting a pre-obtained bearer token -- only account_key/sas_token (raw
        # secrets) or client_id+client_secret+tenant_id (build a *new* credential via
        # service-principal flow, not usable with an already-vended token). A UC-vended
        # azure_aad token has nowhere to go; this falls back to ambient DefaultAzureCredential
        # resolution, which will fail closed (permission error) if that doesn't separately
        # have access. Document as a known Phase 3 Azure gap, not silently swallowed.
        return _VendedCredential({}, {}, resp.expiration_time)
    return None    # unrecognized shape -- vend_credential() logs this, not this pure mapper


def vend_credential(path: str):
    """Call UC's generate_temporary_path_credentials for `path`. Returns a _VendedCredential,
    or None on ANY failure (not on Databricks, SDK missing, network, permissions, path not
    UC-governed) -- every failure mode is treated identically: fall back to ambient
    credentials, never raise. A hard failure here would break every cloud read on a
    Databricks cluster that simply isn't UC-governed for this particular path.

    Every non-trivial failure is logged at WARNING (not just silently returned as None) --
    the fallback-to-ambient behavior is deliberate and correct (see above), but a silent
    fallback that then also fails (ambient resolution picking up an unauthorized identity,
    e.g. serverless compute's own base infrastructure role rather than the caller's
    UC-governed storage credential) previously surfaced only as an opaque AWS/GCS permission
    error several layers away, with zero indication that vending was even attempted, let
    alone why it didn't produce a usable credential."""
    if not is_databricks():
        return None
    sdk = _import_databricks_sdk()
    if sdk is None:
        _logger.info(
            "litebfx: running on Databricks but databricks-sdk isn't importable, so Unity "
            "Catalog credential vending is skipped for %r -- falling back to ambient "
            "credential resolution. It's normally preinstalled on Databricks Runtime/"
            "Serverless; if this environment genuinely lacks it and this path needs a "
            "UC-vended credential rather than an ambient one, pip install databricks-sdk.",
            path)
        return None
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.catalog import PathOperation
        w = WorkspaceClient()
        resp = w.temporary_path_credentials.generate_temporary_path_credentials(
            path, PathOperation.PATH_READ)
        cred = _credential_from_response(resp)
        if cred is None:
            _logger.warning("litebfx: Unity Catalog vending returned no usable credential for "
                             "%r -- falling back to ambient credential resolution.", path)
        return cred
    except Exception:
        _logger.warning(
            "litebfx: Unity Catalog credential vending failed for %r -- falling back to "
            "ambient credential resolution, which may not be authorized for this path. Common "
            "causes: the path isn't under a registered External Location, the caller isn't "
            "granted READ FILES on it, or WorkspaceClient() auth didn't resolve in this "
            "process (see tests/smoke_uc_credential_vending.py).", path, exc_info=True)
        return None


def credentials_for(path: str):
    """Cached vended credential for path's bucket; re-vends when missing or expired
    (respecting expiration_time's own advice to re-vend near expiry, not vend-once)."""
    key = _io.bucket_key(path)[:2]
    cached = _CACHE.get(key)
    if cached is None or cached.is_expired():
        cached = vend_credential(path)
        _CACHE[key] = cached
    return cached


def prepare_env(path: str) -> None:
    """Ensure the process environment has whatever htslib's S3/GCS backend needs to open
    `path`. No-op for local paths and for Azure (download mode -- Azure never reaches
    htslib).

    Independent things happen here for a "native" cloud path:
    - On Databricks, if a vended credential is available, its env vars take priority over
      everything else below (it's scoped to exactly what the caller is authorized for). Off
      Databricks, SDK missing, or vending failed: skipped.
    - For a GCS path specifically, if Databricks vending didn't already supply a
      GCS_OAUTH_TOKEN, GOOGLE_APPLICATION_CREDENTIALS is minted into one when set and the
      `gcp` extra is installed (see _mint_ambient_gcs_token()) -- this is the fallback for
      everyone off Databricks, since GOOGLE_APPLICATION_CREDENTIALS itself is not something
      htslib's GCS backend can use directly. Missing extra, missing env var, or a minting
      failure: skipped, htslib resolves GCS_OAUTH_TOKEN from the ambient environment itself
      (i.e. does nothing further -- same as if this bullet didn't exist).
    - If AWS_ENDPOINT_URL is set (a non-AWS S3-compatible endpoint), HTS_S3_HOST and
      HTS_S3_ADDRESS_STYLE are derived from it, since htslib has no other way to learn a
      non-default host. This alone is *not* enough for htslib to actually use that host --
      the path itself needs the s3+http/s3+https scheme too; see effective_path().

    Called once per _open()-equivalent call, immediately before the pysam constructor -- see
    _base.import_pysam()'s docstring for why "prep right before pysam touches the OS" is this
    package's established pattern. Caller must hold cloud_read_lock() for the duration of the
    read that follows -- see module docstring.
    """
    if _io.cloud_read_mode(path) != "native":
        return
    _fix_ca_bundle_path()
    vended_gcs_token = False
    if is_databricks():
        cred = credentials_for(path)
        if cred is not None and cred.env:
            os.environ.update(cred.env)
            vended_gcs_token = "GCS_OAUTH_TOKEN" in cred.env
    if _io.scheme(path) in _GCS_SCHEMES and not vended_gcs_token:
        cred = _mint_ambient_gcs_token()
        if cred is not None:
            os.environ.update(cred.env)
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint and _io.scheme(path) in _S3_SCHEMES:
        os.environ["HTS_S3_HOST"] = urllib.parse.urlparse(endpoint).netloc
        os.environ.setdefault("HTS_S3_ADDRESS_STYLE", "path")


_CA_BUNDLE_CHECKED = False


def _fix_ca_bundle_path():
    """pysam's manylinux wheel bundles a libcurl with a compiled-in CURLOPT_CAINFO default of
    /etc/pki/tls/certs/ca-bundle.crt (the RedHat/CentOS path) -- not env-var-overridable
    (CURL_CA_BUNDLE/SSL_CERT_FILE do nothing, confirmed empirically: htslib calls libcurl's C
    API directly, not the `curl` CLI tool that actually honors those env vars). On
    Debian/Ubuntu -- including Databricks Runtime, confirmed on 17.3-LTS, this is not a
    Databricks-specific problem -- the real bundle lives at
    /etc/ssl/certs/ca-certificates.crt, so a real (non-MinIO) HTTPS S3/GCS connection fails
    the TLS handshake, and htslib reports it as an opaque "No such file or directory", not a
    certificate error.

    Best-effort and silent on failure (read-only filesystem, non-root, a race with another
    process doing the same fix) -- no worse off than today's broken behavior if it can't fix
    it. Checked at most once per process. See python/README.md's "Known issues" for the
    Dockerfile-level fix this duplicates for images that can't be modified at import time.
    """
    global _CA_BUNDLE_CHECKED
    if _CA_BUNDLE_CHECKED:
        return
    _CA_BUNDLE_CHECKED = True
    target = "/etc/pki/tls/certs/ca-bundle.crt"
    if os.path.exists(target):
        return
    for source in ("/etc/ssl/certs/ca-certificates.crt",    # Debian / Ubuntu / Databricks
                    "/etc/ssl/cert.pem"):                     # Alpine
        if os.path.exists(source):
            try:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                os.symlink(source, target)
            except OSError:
                pass    # non-root, read-only fs, lost a race -- fine, best-effort
            return


def effective_path(path: str) -> str:
    """The path to actually hand to pysam -- identical to `path` except for a "native" S3
    path, which may need rewriting:

    - s3a:// and s3n:// are normalized to s3:// (htslib recognizes only s3/s3+http/s3+https;
      s3a/s3n give "Protocol not supported", confirmed empirically).
    - If AWS_ENDPOINT_URL is set (see prepare_env()), the scheme becomes s3+http:// or
      s3+https:// (matching the endpoint's own protocol) -- htslib's URL syntax is
      `s3[+scheme]://bucket/key` with *no host in the URL at all*; the actual server address
      comes only from HTS_S3_HOST. Plain s3:// always targets real AWS regardless of
      HTS_S3_HOST (confirmed empirically against MinIO), so reaching a custom endpoint
      requires both the env var (prepare_env) and this scheme rewrite.

    Call this on every path handed to pysam -- the main file *and* any index -- after
    prepare_env() has run (order matters: this reads AWS_ENDPOINT_URL, which prepare_env
    doesn't set, only reads, so either order actually works, but keep them adjacent).
    """
    if _io.scheme(path) not in _S3_SCHEMES:
        return path
    _, bucket, key = _io.bucket_key(path)
    tail = f"{bucket}/{key}" if key else bucket
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        return f"s3://{tail}"
    new_scheme = "s3+https" if urllib.parse.urlparse(endpoint).scheme == "https" else "s3+http"
    return f"{new_scheme}://{tail}"
