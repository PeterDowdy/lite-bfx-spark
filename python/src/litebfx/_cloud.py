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

Databricks S3 credential vending, take two -- vend on the driver, thread the result to
workers. A first attempt (Unity Catalog's generate_temporary_path_credentials, called
independently from inside each worker via databricks-sdk's auto-authenticating
WorkspaceClient()) was implemented and then removed: a live diagnostic on a real Databricks
Serverless workspace confirmed the isolated Python Data Source worker subprocess has no
ambient Databricks credential of any kind, and databricks-sdk's "runtime native" auth
strategy depends on an internal `dbruntime` package that isn't present in that stripped-down
venv either. Databricks support's own recommended fix, confirmed working for the reporting
workspace: fetch short-lived AWS STS credentials *once*, on the driver, via a raw REST call
authenticated through the notebook context's own API token (dbutils -- not databricks-sdk,
which this project deliberately doesn't depend on, see pyproject.toml's comment) -- then
thread the resulting plain credential *values* (not a live SDK client) to workers, which only
need to set three env vars, never authenticate to Databricks themselves.

This changes where credential-carrying data lives relative to the driver/worker boundary: a
_DatabricksPathCredential is attached directly to each cloud InputPartition (see bam.py etc.)
-- InputPartition is the one channel Python Data Source guarantees crosses that boundary
intact, since the executor calls DataSource.reader(schema) fresh, reusing only what
partitions() actually returned. This is exactly what the *original* module docstring's design
note warned against ("credential-bearing field is a real exposure risk" in Spark's plan
explain output and worker tracebacks) -- still true, but no longer avoidable now that vending
must happen driver-side and travel with the partition; _DatabricksPathCredential's __repr__/
__str__ redact every secret field instead, so the exposure risk is closed by redaction rather
than by never attaching the credential to a partition at all.

Refresh is bounded by what Python Data Source's design actually allows: partitions() runs
once, up front, before any partition executes, and workers have no channel to call back to
the driver mid-read. credentials_for() is cached per (scheme, bucket) and re-vends whenever
the cached entry is missing or expired, so *every fresh partitions() call* (i.e. every new
Spark action against a cloud path) gets a credential that's fresh as of that moment -- the
finest refresh granularity actually achievable. A single action whose total wall-clock
execution (across all its partitions, stragglers included) exceeds the vended credential's
~1h lifetime will still see mid-read expiry with no way to recover short of re-running the
action; see python/TASKS.md.

Concurrency: os.environ mutation is process-global, and it's unverified whether htslib
re-reads env vars only at open time or on every subsequent range request against an
already-open handle. cloud_read_lock() must be held for the *entire duration* of a
cloud-backed partition's read -- from prepare_env() through exhausting that partition's read
generator, not just around the open call -- so cloud-backed reads are serialized within one
worker process regardless of worker threading model or htslib's actual env-read timing.
Local-path reads never touch this lock.
"""

import contextlib
import json
import os
import threading
import time
import urllib.parse
import urllib.request

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

# Set for the duration of an enclosing cloud_read_scope(), so _cloudfs.filesystem_for() can
# pick up a partition's driver-vended credential during a worker-side read without needing
# every _cloudfs.py function to take a new parameter. Not thread-safe by design -- matches
# _LOCK's existing guarantee that only one cloud-backed read is ever in flight per worker
# process at a time. None outside any cloud_read_scope() (e.g. driver-side planning calls),
# where _cloudfs falls back to calling credentials_for() itself instead.
_ACTIVE_CREDENTIAL = None

# Ambient GCS token minted from GOOGLE_APPLICATION_CREDENTIALS. A single process-wide
# credential, not per-bucket: one service-account key file mints one token usable against
# every bucket that key is authorized for. None means "not yet minted"; a _VendedCredential
# that is_expired() means "mint again". See _mint_ambient_gcs_token().
_AMBIENT_GCS_CACHE = None

# Driver-only: vended AWS credential per (scheme, bucket). See databricks_credential_for().
_DATABRICKS_CRED_CACHE = {}


def cloud_read_lock():
    """The lock callers must hold for the full duration of a cloud-backed partition's read --
    see module docstring."""
    return _LOCK


def active_credential():
    """The credential attached by the innermost enclosing cloud_read_scope(), or None outside
    one. See that function and _cloudfs.filesystem_for()."""
    return _ACTIVE_CREDENTIAL


@contextlib.contextmanager
def cloud_read_scope(path: str, credential=None):
    """prepare_env(path, credential) + hold cloud_read_lock(), for a "native" cloud path; a
    no-op (no lock, no env prep) for a local path. Wrap a format reader's entire
    open-through-generator-exhaustion body in this -- it's a generator-safe context manager
    (the lock stays held across `yield`s, released only when the wrapped generator is
    exhausted or closed), which is what "entire duration of a partition's read" requires.
    Azure (download mode) doesn't need this: it never touches os.environ.

    `credential`, if given, is a partition's driver-vended _DatabricksPathCredential (see
    bam.py etc. for where it's attached) -- also exposed for the duration of this scope via
    active_credential(), so _cloudfs.py's pyarrow.fs calls (e.g. a byte-range read that never
    goes through prepare_env()'s htslib env vars at all) can pick it up too.

    This only sets up the *environment* -- callers must still pass every path they open
    (the main file AND any index) through effective_path() before handing it to pysam; see
    that function's docstring for why the path itself, not just env vars, can need rewriting.
    """
    global _ACTIVE_CREDENTIAL
    if _io.cloud_read_mode(path) == "native":
        with _LOCK:
            prepare_env(path, credential)
            _ACTIVE_CREDENTIAL = credential
            try:
                yield
            finally:
                _ACTIVE_CREDENTIAL = None
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


def is_databricks() -> bool:
    """True when running on a Databricks cluster/serverless process, driver or worker --
    DATABRICKS_RUNTIME_VERSION is set on every node. Only actually useful driver-side for
    credential vending (see module docstring) -- worker-side callers always get a no-op
    from databricks_credential_for() regardless, since _active_spark_session() fails closed
    there, but checking this first avoids that attempt in the common non-Databricks case."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


class _DatabricksPathCredential:
    """A short-lived AWS STS credential vended by Unity Catalog's Temporary Path Credentials
    API, attached to a cloud InputPartition so a worker can use it without ever
    authenticating to Databricks itself (see module docstring for why that's necessary and
    _vend_databricks_path_credential() for how it's obtained).

    __repr__/__str__ deliberately redact every secret field: this object is a dataclass field
    on real InputPartition subclasses (see bam.py etc.), which Spark can print in full via
    plan explain output or a worker traceback on a failed task. Only the non-secret
    expiration timestamp is shown.
    """
    __slots__ = ("access_key_id", "secret_access_key", "session_token", "expiration_time_ms")

    def __init__(self, access_key_id, secret_access_key, session_token, expiration_time_ms):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.expiration_time_ms = expiration_time_ms

    def __repr__(self):
        return (f"{self.__class__.__name__}(<redacted>, "
                f"expiration_time_ms={self.expiration_time_ms!r})")

    __str__ = __repr__

    def is_expired(self, skew_seconds=60) -> bool:
        if self.expiration_time_ms is None:
            return False
        return time.time() * 1000 >= self.expiration_time_ms - skew_seconds * 1000

    @property
    def env(self):
        d = {"AWS_ACCESS_KEY_ID": self.access_key_id,
             "AWS_SECRET_ACCESS_KEY": self.secret_access_key}
        if self.session_token:
            d["AWS_SESSION_TOKEN"] = self.session_token
        return d

    @property
    def fs_kwargs(self):
        d = {"access_key": self.access_key_id, "secret_key": self.secret_access_key}
        if self.session_token:
            d["session_token"] = self.session_token
        return d


def _active_spark_session():
    """The active SparkSession, or None. Driver-only in practice: an isolated Python Data
    Source worker subprocess has no JVM gateway at all, so this fails closed (returns None,
    never raises) there -- which is exactly what makes it safe to call unconditionally from
    _cloudfs.py without separately tracking "am I on the driver"."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:
        return None


def _databricks_notebook_context(spark):
    """(workspace_url, api_token) via the notebook context's own ephemeral token, or None.

    Deliberately not databricks-sdk's WorkspaceClient() -- per Databricks support's own
    recommendation, since (a) litebfx doesn't depend on databricks-sdk at all, on purpose
    (see pyproject.toml's comment on the setuptools conflict that caused its removal as an
    extra) and (b) this is the mechanism Databricks support specifically confirmed works
    driver-side for this exact use case. dbutils is part of Databricks' own PySpark
    distribution already, so this needs no new dependency.

    UNVERIFIED (flagged per this project's practice of being explicit about what's actually
    confirmed against a real workspace vs. inferred): whether a `pyspark.dbutils.DBUtils`
    instance constructed this way (rather than a notebook's own already-injected `dbutils`
    global) supports the same `.notebook.entry_point.getDbutils()...` chain. Tries the
    IPython user-namespace `dbutils` first (the real, notebook-injected object, when this
    code happens to run from an actual notebook cell) and falls back to constructing one
    explicitly, but the fallback path specifically has not been independently confirmed.
    Returns None on ANY failure -- never raises; a failure here just means vending is
    skipped and callers fall back to ambient credential resolution, same as everywhere else
    in this module.
    """
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        return None
    if not workspace_url:
        return None
    dbutils = None
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip is not None:
            dbutils = ip.user_ns.get("dbutils")
    except Exception:
        pass
    if dbutils is None:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except Exception:
            return None
    try:
        token = (dbutils.notebook.entry_point.getDbutils()
                 .notebook().getContext().apiToken().get())
    except Exception:
        return None
    return (workspace_url, token) if token else None


def _vend_databricks_path_credential(path: str, operation: str = "PATH_READ"):
    """Raw REST call to Unity Catalog's Temporary Path Credentials API
    (POST /api/2.1/unity-catalog/temporary-path-credentials), authenticated via
    _databricks_notebook_context(). Uses only the standard library (urllib, json) -- no new
    dependency. AWS/S3 only for now (aws_temp_credentials) -- this is what the reporting
    workspace actually needed and what Databricks support's guidance covered; GCS/Azure
    likely return an analogous shape (gcp_oauth_token / azure_*) through this same endpoint,
    matching what the removed databricks-sdk-based implementation used to parse, but that's
    unconfirmed through this raw-REST path specifically and not wired up here.

    DRIVER-ONLY -- must never be called from read()/worker code (no SparkSession there to
    get a notebook context from in the first place; see _active_spark_session()). Returns
    None on ANY failure, never raises: not on Databricks, no notebook context, network
    error, permission denied (the identity needs EXTERNAL USE LOCATION on the external
    location backing this path -- a separate grant from the READ FILES this project's
    removed UC-vending implementation needed, easy to miss), unrecognized response shape.
    """
    spark = _active_spark_session()
    if spark is None:
        return None
    ctx = _databricks_notebook_context(spark)
    if ctx is None:
        return None
    workspace_url, token = ctx
    _, bucket, key = _io.bucket_key(path)
    url = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
    body = json.dumps({"url": url, "operation": operation}).encode()
    req = urllib.request.Request(
        f"https://{workspace_url}/api/2.1/unity-catalog/temporary-path-credentials",
        data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
    except Exception:
        return None
    aws = payload.get("aws_temp_credentials")
    if not aws or not aws.get("access_key_id") or not aws.get("secret_access_key"):
        return None
    return _DatabricksPathCredential(
        aws["access_key_id"], aws["secret_access_key"], aws.get("session_token"),
        payload.get("expiration_time"))


def databricks_credential_for(path: str):
    """Cached driver-vended AWS credential for path's bucket, or None (not on Databricks,
    vending unavailable/failed, or path isn't S3). Re-vends whenever the cached entry is
    missing or expired -- see module docstring for the refresh-granularity this actually
    achieves (once per fresh partitions() call, not mid-read).

    DRIVER-ONLY in effect: _vend_databricks_path_credential() needs a real SparkSession,
    which _active_spark_session() only ever finds on the driver -- calling this from worker
    code is safe (never raises) but always returns whatever's already cached, without ever
    being able to populate the cache itself.
    """
    if not is_databricks() or _io.scheme(path) not in _S3_SCHEMES:
        return None
    key = _io.bucket_key(path)[:2]
    cached = _DATABRICKS_CRED_CACHE.get(key)
    if cached is None or cached.is_expired():
        cached = _vend_databricks_path_credential(path)
        _DATABRICKS_CRED_CACHE[key] = cached
    return cached


def prepare_env(path: str, credential=None) -> None:
    """Ensure the process environment has whatever htslib's S3/GCS backend needs to open
    `path`. No-op for local paths and for Azure (download mode -- Azure never reaches
    htslib).

    Independent things happen here for a "native" cloud path:
    - If `credential` is given (a partition's driver-vended _DatabricksPathCredential -- see
      module docstring), its env vars are applied and take priority over everything below.
    - Otherwise, for a GCS path specifically, GOOGLE_APPLICATION_CREDENTIALS is minted into a
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
    if credential is not None:
        os.environ.update(credential.env)
    elif _io.scheme(path) in _GCS_SCHEMES:
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
