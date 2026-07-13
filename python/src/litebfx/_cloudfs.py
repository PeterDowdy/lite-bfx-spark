"""pyarrow.fs dispatch: local-vs-cloud filesystem operations behind one API.

Every function here dispatches on ``io.is_cloud_path()`` internally, so callers
(``_base.py``, ``bgzf.py``, format readers) never branch on local-vs-cloud themselves. No
Databricks or credential-vending knowledge lives here — see ``_cloud.py`` for that; this
module only turns a cloud URI into bytes via ``pyarrow.fs``, using whatever credentials
``_cloud.py`` (or pyarrow's own ambient resolution) has already made available.

A non-AWS S3-compatible endpoint (MinIO, on-prem Ceph, etc.) needs the standard
``AWS_ENDPOINT_URL`` env var -- ``pyarrow.fs.FileSystem.from_uri()``'s ambient resolution
reads it directly, confirmed empirically against MinIO, no code branch needed here. This is
separate from htslib's own ``HTS_S3_HOST``/``HTS_S3_ADDRESS_STYLE`` (see ``_cloud.py``) --
both need setting for a non-AWS endpoint, since htslib and pyarrow.fs resolve S3 config
independently of each other.

GCS has no equivalent ambient env var (confirmed: ``GcsFileSystem``'s only auth path is the
standard GCP credential chain via ``GOOGLE_APPLICATION_CREDENTIALS``, with no endpoint
override read from the environment) -- ``from_uri()`` against a non-Google endpoint like
fake-gcs-server tries to reach real Google OAuth2 servers and fails (confirmed empirically:
a 30+ minute retry-exhaustion hang, not a fast failure). ``GCS_ENDPOINT_URL`` (a litebfx-only
convention, no upstream standard exists) triggers an explicit ``GcsFileSystem(scheme=...,
endpoint_override=..., anonymous=True)`` construction instead -- ``anonymous=True`` because a
non-Google endpoint set this way is almost always a local emulator or on-prem GCS-API-
compatible store that doesn't enforce real GCP auth, not authenticated production GCS.

Azure (``AzureFileSystem``) needs its ``account_name`` as a constructor argument, not parsed
from a bucket the way S3/GCS are -- see ``io.azure_parts()``. Ambient auth (no kwargs beyond
`account_name`) uses `DefaultAzureCredential`'s chain (managed identity, `az` CLI, standard
`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET`/`AZURE_TENANT_ID` env vars for a service principal) --
none of that covers a raw storage *account key* (Azurite's auth model, and many on-prem/
non-AAD Azure-API-compatible stores), which has no ambient env var path in pyarrow.fs at all,
so ``AZURE_STORAGE_ACCOUNT_KEY``/``AZURE_STORAGE_ENDPOINT_URL`` (litebfx-only conventions,
mirroring ``GCS_ENDPOINT_URL``) are read explicitly here, same pattern as GCS.
"""

import io as _pyio
import os
import shutil
import tempfile
import threading
import urllib.parse

import pyarrow.fs

from . import _cloud
from . import io as _io

_FS_CACHE = {}   # (scheme, bucket-or-account, credential-id-or-None) -> pyarrow.fs.FileSystem

_AZURE_SCHEMES = ("abfs", "abfss", "wasb", "wasbs")

# scheme -> pyarrow.fs constructor, for the credentialed (Phase 2/3) construction path.
_FS_CTOR = {
    "s3": pyarrow.fs.S3FileSystem, "s3a": pyarrow.fs.S3FileSystem, "s3n": pyarrow.fs.S3FileSystem,
    "gs": pyarrow.fs.GcsFileSystem, "gcs": pyarrow.fs.GcsFileSystem,
}


def _cache_fs(cache_key, fs):
    """Insert `fs` under `cache_key` and drop any other entry sharing the same (scheme,
    bucket-or-account) prefix -- a credential refresh changes `cache_key`'s third element
    (see filesystem_for()'s docstring), so without this, _FS_CACHE would grow by one stale,
    never-reused entry per re-vend over a long-running worker process instead of staying at
    one live entry per bucket."""
    prefix = cache_key[:2]
    for other in [k for k in _FS_CACHE if k[:2] == prefix and k != cache_key]:
        del _FS_CACHE[other]
    _FS_CACHE[cache_key] = fs


def filesystem_for(path: str):
    """Return (pyarrow.fs.FileSystem, within-fs-path) for a cloud URI.

    The filesystem is memoized per (scheme, bucket-or-account, credential identity):
    constructing one does real credential resolution, not free. Ambient credential resolution
    (env vars, instance metadata, shared config) happens via `pyarrow.fs.FileSystem.from_uri`
    for S3/GCS, with no arguments needed -- unless `_cloud.credentials_for()` has a vended
    credential for this bucket (Databricks UC), in which case the scheme-specific FileSystem
    is constructed with explicit credential kwargs instead, taking priority over ambient
    resolution. Azure always needs explicit construction (see module docstring) -- see
    `_azure_filesystem_for`.

    The credential identity is part of the cache key -- not just (scheme, bucket) -- so a
    freshly re-vended credential (near-expiry refresh, or a retry after a transient vending
    failure) naturally invalidates the old entry instead of the stale filesystem being reused
    forever. Without this, a single transient vend_credential() failure on the *first* cloud
    touch in a worker process (network blip, WorkspaceClient() not yet warmed up) would
    permanently pin that bucket to the unauthorized ambient-fallback filesystem for the rest
    of the process's life, even though _cloud.credentials_for() itself retries correctly on
    every call -- confirmed by reading credentials_for()'s own cache logic, which re-attempts
    vending whenever the cached entry is None, not just on expiry. This bug's symptom looks
    exactly like a permissions problem (a clean, consistent AWS ACCESS_DENIED / 403) rather
    than the transient-hiccup-then-cache-poisoning it actually is.

    Raises ValueError if `path` is not a cloud path litebfx recognizes.
    """
    s = _io.scheme(path)
    if s in _AZURE_SCHEMES:
        return _azure_filesystem_for(path)
    if s not in _FS_CTOR:
        raise ValueError(f"not a cloud path: {path!r}")
    _, bucket, key = _io.bucket_key(path)
    cred = _cloud.credentials_for(path) if _cloud.is_databricks() else None
    cache_key = (s, bucket, id(cred) if cred is not None else None)
    fs = _FS_CACHE.get(cache_key)
    if fs is None:
        gcs_endpoint = os.environ.get("GCS_ENDPOINT_URL") if s in ("gs", "gcs") else None
        if cred is not None and cred.fs_kwargs:
            fs = _FS_CTOR[s](**cred.fs_kwargs)
        elif gcs_endpoint:
            parsed = urllib.parse.urlparse(gcs_endpoint)
            fs = pyarrow.fs.GcsFileSystem(
                anonymous=True, scheme=parsed.scheme or "http", endpoint_override=parsed.netloc)
        else:
            fs, _ = pyarrow.fs.FileSystem.from_uri(f"{s}://{bucket}")
        _cache_fs(cache_key, fs)
    return fs, f"{bucket}/{key}" if key else bucket


def _azure_filesystem_for(path: str):
    account_name, container, key = _io.azure_parts(path)
    cred = _cloud.credentials_for(path) if _cloud.is_databricks() else None
    cache_key = (_io.scheme(path), account_name, id(cred) if cred is not None else None)
    fs = _FS_CACHE.get(cache_key)
    if fs is None:
        kwargs = {}
        if cred is not None and cred.fs_kwargs:
            kwargs.update(cred.fs_kwargs)
        endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT_URL")
        if endpoint:
            parsed = urllib.parse.urlparse(endpoint)
            scheme = parsed.scheme or "http"
            kwargs.setdefault("blob_storage_authority", parsed.netloc)
            kwargs.setdefault("dfs_storage_authority", parsed.netloc)
            kwargs.setdefault("blob_storage_scheme", scheme)
            kwargs.setdefault("dfs_storage_scheme", scheme)
        account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        if account_key and "sas_token" not in kwargs:
            kwargs.setdefault("account_key", account_key)
        fs = pyarrow.fs.AzureFileSystem(account_name, **kwargs)
        _cache_fs(cache_key, fs)
    return fs, f"{container}/{key}" if key else container


def exists(path: str) -> bool:
    """os.path.exists() for a local or cloud path."""
    if not _io.is_cloud_path(path):
        return os.path.exists(path)
    fs, within = filesystem_for(path)
    return fs.get_file_info(within).type != pyarrow.fs.FileType.NotFound


def isdir(path: str) -> bool:
    """os.path.isdir() for a local or cloud "directory" (a key prefix with objects under it,
    per pyarrow.fs's own directory inference for object storage)."""
    if not _io.is_cloud_path(path):
        return os.path.isdir(path)
    fs, within = filesystem_for(path)
    return fs.get_file_info(within).type == pyarrow.fs.FileType.Directory


def getsize(path: str) -> int:
    """os.path.getsize() for a local or cloud path."""
    if not _io.is_cloud_path(path):
        return os.path.getsize(path)
    fs, within = filesystem_for(path)
    return fs.get_file_info(within).size


def stat(path: str):
    """(size, mtime_or_None) -- os.stat()-equivalent for a local or cloud path."""
    if not _io.is_cloud_path(path):
        st = os.stat(path)
        return st.st_size, st.st_mtime
    fs, within = filesystem_for(path)
    info = fs.get_file_info(within)
    return info.size, (info.mtime.timestamp() if info.mtime else None)


def listdir(path: str) -> list:
    """os.listdir()-equivalent: one-level directory/prefix listing, bare names (not full
    paths) -- matches os.listdir()'s contract so callers' existing os.path.join(path, name)
    logic works unchanged for cloud paths too."""
    if not _io.is_cloud_path(path):
        return os.listdir(path)
    fs, within = filesystem_for(path)
    selector = pyarrow.fs.FileSelector(within, recursive=False)
    return [info.path.rstrip("/").rsplit("/", 1)[-1] for info in fs.get_file_info(selector)]


def open_read_range(path: str, start: int, length: int) -> bytes:
    """Read up to `length` bytes starting at `start`. Local: open+seek+read. Cloud:
    pyarrow.fs.open_input_file(...).seek(start); .read(length)."""
    if not _io.is_cloud_path(path):
        with open(path, "rb") as f:
            f.seek(start)
            return f.read(length)
    fs, within = filesystem_for(path)
    with fs.open_input_file(within) as f:
        f.seek(start)
        return f.read(length)


def open_stream(path: str):
    """A seek()+readline()-capable binary file object for a local or cloud path."""
    if not _io.is_cloud_path(path):
        return open(path, "rb")
    fs, within = filesystem_for(path)
    # pyarrow.NativeFile supports seek/read directly; wrapping in BufferedReader guarantees
    # readline() behaves like a normal Python binary file object (the documented pattern for
    # using a NativeFile as a drop-in file-like object).
    return _pyio.BufferedReader(fs.open_input_file(within))


_MATERIALIZED = {}   # source path -> local temp path, process-lifetime cache
_MATERIALIZE_LOCK = threading.Lock()


def _temp_suffix(path: str) -> str:
    """Preserve a possibly-compound suffix (.vcf.gz, .bam.bai) on the temp file, since some
    pysam/htslib format detection is suffix-sensitive."""
    name = path.rsplit("/", 1)[-1]
    return "." + name.split(".", 1)[1] if "." in name else ""


def materialize_local(path: str) -> str:
    """Download a cloud object (Azure -- no native htslib backend, see io.py) to a local temp
    file, returning the local path. Cached by source path for the worker process's lifetime:
    re-downloading the same file on every partition/open within one process would be
    wasteful. Cleanup relies on process/container lifecycle, not explicit deletion -- a
    long-running worker touching many distinct large files will accumulate local temp
    storage; see TASKS.md for this as a documented, accepted v1 limitation.

    Locked (not just cache-checked) because a concurrent duplicate download of the same file
    -- e.g. two partitions racing on the same shared index file -- would be wasted work, not
    a correctness bug, but avoiding it is cheap.
    """
    with _MATERIALIZE_LOCK:
        cached = _MATERIALIZED.get(path)
        if cached is not None and os.path.exists(cached):
            return cached
        fs, within = filesystem_for(path)
        fd, local_path = tempfile.mkstemp(suffix=_temp_suffix(path), prefix="litebfx-")
        with os.fdopen(fd, "wb") as out, fs.open_input_stream(within) as src:
            shutil.copyfileobj(src, out)
        _MATERIALIZED[path] = local_path
        return local_path


def resolve_open_path(path: str) -> str:
    """The path to actually hand to pysam for any cloud path -- unchanged for local,
    `_cloud.effective_path()`'s rewrite for "native" S3/GCS, `materialize_local()`'s local
    temp copy for "download" mode (Azure). One call covers every cloud_read_mode(); format
    readers call this instead of branching on mode themselves. Lives here rather than in
    _cloud.py because it needs materialize_local(), and _cloud.py must not import _cloudfs
    (the reverse edge -- _cloudfs already imports _cloud for credentials -- would cycle)."""
    mode = _io.cloud_read_mode(path)
    if mode == "native":
        return _cloud.effective_path(path)
    if mode == "download":
        return materialize_local(path)
    return path
