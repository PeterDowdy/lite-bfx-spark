"""Cloud credentials for htslib's native S3/GCS remote-read backend, and for the pyarrow.fs
filesystems _cloudfs.py uses for orchestration (including Azure's download-fallback path).

For S3 and Azure, ambient credentials (env vars, instance metadata, shared config) need
nothing from this module -- htslib and pyarrow.fs both resolve them on their own. GCS is a
real exception: htslib's native GCS backend wants an already-minted OAuth *access token* in
GCS_OAUTH_TOKEN, not GOOGLE_APPLICATION_CREDENTIALS (a key *file path*) -- the credential
shape most GCS users actually have ambiently, and the one pyarrow.fs.GcsFileSystem itself
resolves happily on its own. Left alone, that's a silent read failure for anyone with only a
service-account key file. prepare_env() closes this: when GOOGLE_APPLICATION_CREDENTIALS is
set and the `gcp` extra (google-auth) is installed, it mints an access token from the key
file directly (see _mint_ambient_gcs_token()) -- cached process-wide and refreshed near
expiry. Missing extra, missing env var, or any minting failure is a silent no-op, same as the
rest of this module's failure posture: htslib/pyarrow.fs just fall back to whatever's already
ambient.

No Databricks-specific credential vending: Unity Catalog's Temporary Credentials API
(generate_temporary_path_credentials) was implemented and then removed after confirming, via
a diagnostic run inside an isolated Python Data Source worker subprocess on a real Databricks
Serverless workspace, that WorkspaceClient()'s default auth cannot resolve there at all --
the worker environment has no ambient DATABRICKS_HOST/TOKEN/CLIENT_ID/CLIENT_SECRET of any
kind, and the SDK's "runtime native" auth strategy depends on an internal `dbruntime` package
that isn't present in that stripped-down venv either. See python/TASKS.md's "Implemented
since first cut" section for the full investigation (multiple real bugs were found and fixed
along the way -- a stale filesystem cache, unnecessary hard dependencies on pyarrow/
databricks-sdk, a genuine SDK-version gap -- before this final, structural blocker surfaced).
Ambient credentials (an instance-profile-configured classic cluster, or a manually-provisioned
service principal's DATABRICKS_CLIENT_ID/SECRET injected into the compute environment) are
the only way to authenticate direct cloud reads on Databricks going forward.

Concurrency: os.environ mutation is process-global, and it's unverified whether htslib
re-reads env vars only at open time or on every subsequent range request against an
already-open handle. cloud_read_lock() must be held for the *entire duration* of a
cloud-backed partition's read -- from prepare_env() through exhausting that partition's read
generator, not just around the open call -- so cloud-backed reads are serialized within one
worker process regardless of worker threading model or htslib's actual env-read timing.
Local-path reads never touch this lock.
"""

import contextlib
import os
import threading
import time
import urllib.parse

from . import io as _io

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

# Ambient GCS token minted from GOOGLE_APPLICATION_CREDENTIALS. A single process-wide
# credential, not per-bucket: one service-account key file mints one token usable against
# every bucket that key is authorized for. None means "not yet minted"; a _VendedCredential
# that is_expired() means "mint again". See _mint_ambient_gcs_token().
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


def _import_google_auth():
    """Lazy import, same pattern as _base.import_pysam() -- returns None (not a raise)
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
        self.env = env                            # for htslib (GCS only) and for pyarrow.fs
        self.fs_kwargs = fs_kwargs
        self.expiration_time_ms = expiration_time_ms

    def is_expired(self, skew_seconds=60) -> bool:
        if self.expiration_time_ms is None:
            return False
        return time.time() * 1000 >= self.expiration_time_ms - skew_seconds * 1000


def _mint_ambient_gcs_token():
    """Mint (or reuse a still-valid cached) OAuth access token from
    GOOGLE_APPLICATION_CREDENTIALS, via google-auth. Read-only scope -- this package never
    writes to cloud storage.

    Returns None on ANY failure (GOOGLE_APPLICATION_CREDENTIALS unset, `gcp` extra not
    installed, malformed key file, network) -- every failure mode falls back to whatever's
    already ambient, never raises. Cached process-wide in _AMBIENT_GCS_CACHE: one
    service-account key mints one token usable against any bucket that key can reach, so
    there's no per-bucket cache key needed.
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


def prepare_env(path: str) -> None:
    """Ensure the process environment has whatever htslib's S3/GCS backend needs to open
    `path`. No-op for local paths and for Azure (download mode -- Azure never reaches
    htslib).

    Independent things happen here for a "native" cloud path:
    - For a GCS path specifically, GOOGLE_APPLICATION_CREDENTIALS is minted into a
      GCS_OAUTH_TOKEN when set and the `gcp` extra is installed (see
      _mint_ambient_gcs_token()), since GOOGLE_APPLICATION_CREDENTIALS itself is not
      something htslib's GCS backend can use directly. Missing extra, missing env var, or a
      minting failure: skipped, htslib resolves GCS_OAUTH_TOKEN from the ambient environment
      itself (i.e. does nothing further -- same as if this bullet didn't exist).
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
    if _io.scheme(path) in _GCS_SCHEMES:
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
