"""Filesystem and cloud path handling.

Local paths are opened directly. ``s3://``/``s3a://``/``s3n://``/``gs://``/``gcs://`` are
opened directly too: pysam's bundled htslib has a native remote-read backend for S3 and GCS
(SigV4 signing, ``AWS_*``/``GCS_OAUTH_TOKEN`` env vars — see ``_cloud.py``), so no mount is
needed for those. ``abfss://``/``wasbs://`` (and the non-``s``-suffixed variants) are also
opened directly, but through a different mechanism: htslib has **no** native Azure backend
(confirmed by binary inspection), so the object is downloaded to a local temp file via
``pyarrow.fs.AzureFileSystem`` first and pysam opens the local copy — trading the
range-request-only efficiency S3/GCS get for direct ``abfss://`` support with no FUSE mount.
Everything else still requires a FUSE mount:

* ``dbfs:/x``      -> ``/dbfs/x``     (Databricks DBFS FUSE, classic clusters)
* ``file:///x``    -> ``/x``
* ``/Volumes/...`` -> unchanged      (UC Volumes; already a POSIX path)
* local paths      -> unchanged
* ``s3://``, ``s3a://``, ``s3n://``, ``gs://``, ``gcs://`` -> unchanged, opened directly by
  htslib (``"native"`` read mode; see ``_cloud.py`` for credential handling)
* ``abfs://``, ``abfss://``, ``wasb://``, ``wasbs://`` -> unchanged, downloaded then opened
  (``"download"`` read mode; see ``_cloudfs.materialize_local``)
* ``adl://``, ``hdfs://``, ``http(s)://``, ``ftp://`` are **rejected** with guidance to mount
  instead
"""

from urllib.parse import urlparse

# Schemes with a native htslib remote-read backend (verified: SigV4 signing / GCS_OAUTH_TOKEN
# compiled into pysam's bundled htslib) — opened directly, credentials via _cloud.py.
_CLOUD_NATIVE = ("s3", "s3a", "s3n", "gs", "gcs")

# Schemes with no native htslib backend (confirmed absent by binary inspection) but a working
# pyarrow.fs.AzureFileSystem download-then-open path — see _cloudfs.materialize_local.
_CLOUD_DOWNLOAD = ("abfs", "abfss", "wasb", "wasbs")

# Schemes still rejected: no native htslib backend and no orchestration-layer story.
_REJECTED = ("adl", "hdfs", "http", "https", "ftp")


class UnsupportedPathError(ValueError):
    """Raised for a raw object-store URI that must instead be mounted into the filesystem."""


def scheme(path: str) -> str:
    """Lowercase URI scheme of ``path``, or ``""`` for a local/unscoped path."""
    return urlparse(path).scheme.lower()


def is_cloud_path(path: str) -> bool:
    """True for a URI litebfx opens directly (no FUSE mount needed)."""
    return scheme(path) in _CLOUD_NATIVE or scheme(path) in _CLOUD_DOWNLOAD


def cloud_read_mode(path: str):
    """``"native"`` if htslib opens ``path`` itself, ``"download"`` if it's materialized to a
    local temp file first (Azure), else ``None`` (local, mounted, or rejected)."""
    s = scheme(path)
    if s in _CLOUD_NATIVE:
        return "native"
    if s in _CLOUD_DOWNLOAD:
        return "download"
    return None


def bucket_key(path: str):
    """(scheme, bucket, key) for an s3/gs-family URI; ``key`` is ``""`` for a bare-bucket
    path. Not for Azure -- its URL shape (``container@account.dfs.core.windows.net/key``)
    has no single "bucket" segment; see ``azure_parts()``.

    Shared by ``_cloudfs.py`` (filesystem construction) and ``_cloud.py`` (credential-cache
    keying) — lives here, not in either of those, so neither has to import the other just
    for this (``_cloudfs`` -> ``_cloud`` for Phase 2 credentials is the only real edge in
    that graph).
    """
    if "://" not in path:
        raise ValueError(f"not a cloud URI (no scheme): {path!r}")
    s = scheme(path)
    rest = path.split("://", 1)[1]
    bucket, _, key = rest.partition("/")
    return s, bucket, key


def azure_parts(path: str):
    """(account_name, container, key) for an abfss://container@account.dfs.core.windows.net/key
    or wasbs://container@account.blob.core.windows.net/key URI. Azure's URL authority packs
    two identifiers (container, storage account) where S3/GCS have just one (bucket) --
    pyarrow.fs.AzureFileSystem treats the *account* as the filesystem root and containers as
    top-level directories within it, so both need extracting separately, unlike bucket_key().
    """
    _, authority, key = bucket_key(path)
    container, sep, host = authority.partition("@")
    if not sep:
        raise ValueError(f"expected container@account authority in Azure URI: {path!r}")
    account_name = host.split(".", 1)[0]
    return account_name, container, key


def normalize_path(path: str) -> str:
    """Return a path for ``path`` that pysam (possibly via ``_cloud``/``_cloudfs``) can open,
    or raise :class:`UnsupportedPathError`.

    See the module docstring for the mapping.
    """
    if path is None:
        raise ValueError("path is required")

    if path.startswith("dbfs:/"):
        # dbfs:/mnt/x -> /dbfs/mnt/x  (dbfs:/ FUSE mount on classic clusters)
        return "/dbfs" + path[len("dbfs:"):]

    if path.startswith("file:"):
        return urlparse(path).path or path[len("file:"):]

    s = scheme(path)
    if s in _CLOUD_NATIVE:
        return path

    if s in _REJECTED:
        raise UnsupportedPathError(
            f"litebfx does not support {s!r} URIs directly; got {path!r}.\n"
            "s3://, s3a://, and gs:// are supported directly (no mount needed). For "
            "abfss://, wasbs://, and other schemes, mount the storage with a FUSE driver "
            "(blobfuse2, mountpoint-s3, gcsfuse, rclone mount) and pass the mounted path, "
            "or use a Unity Catalog Volume path (/Volumes/<catalog>/<schema>/<volume>/...) "
            "on Databricks."
        )

    # local path, /Volumes/..., or an unknown non-cloud scheme -> pass through unchanged.
    return path


def index_candidates(path: str, suffixes) -> list:
    """Co-located index path candidates for ``path`` (e.g. ``.bai`` -> ``x.bam.bai``)."""
    return [path + s for s in suffixes]
