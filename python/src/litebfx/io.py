"""Filesystem path handling.

The reader opens **local filesystem paths only** and hands them to pysam. Cloud storage is
reached through a FUSE mount (Databricks UC Volumes / DBFS FUSE, or a user-provided
``mountpoint-s3`` / ``gcsfuse`` / ``blobfuse2`` mount), so there is no cloud SDK or
credential handling here — see the proposal's "Cloud I/O" section.

Only path *normalization* happens:

* ``dbfs:/x``      -> ``/dbfs/x``     (Databricks DBFS FUSE, classic clusters)
* ``file:///x``    -> ``/x``
* ``/Volumes/...`` -> unchanged      (UC Volumes; already a POSIX path)
* local paths      -> unchanged
* raw object-store URIs (``s3://``, ``s3a://``, ``abfss://``, ``gs://``, ``wasbs://``,
  ``http(s)://``) are **rejected** with guidance, because their credentials live in the
  JVM/Hadoop layer the Python worker cannot see.
"""

from urllib.parse import urlparse

# Schemes we refuse: their credentials are not visible to a Python worker.
_REJECTED = ("s3", "s3a", "s3n", "abfs", "abfss", "gs", "wasb", "wasbs",
             "adl", "hdfs", "http", "https", "ftp")


class UnsupportedPathError(ValueError):
    """Raised for a raw object-store URI that must instead be mounted into the filesystem."""


def normalize_path(path: str) -> str:
    """Return a local filesystem path for ``path``, or raise :class:`UnsupportedPathError`.

    See the module docstring for the mapping.
    """
    if path is None:
        raise ValueError("path is required")

    if path.startswith("dbfs:/"):
        # dbfs:/mnt/x -> /dbfs/mnt/x  (dbfs:/ FUSE mount on classic clusters)
        return "/dbfs" + path[len("dbfs:"):]

    if path.startswith("file:"):
        return urlparse(path).path or path[len("file:"):]

    scheme = urlparse(path).scheme.lower()
    if scheme in _REJECTED:
        raise UnsupportedPathError(
            f"litebfx reads local filesystem paths only; got {scheme!r} URI {path!r}.\n"
            "On Databricks, use a Unity Catalog Volume path (/Volumes/<catalog>/<schema>/"
            "<volume>/...) or a dbfs:/ path. Off Databricks, mount the bucket with a FUSE "
            "driver (mountpoint-s3, s3fs, gcsfuse, blobfuse2, rclone mount) and pass the "
            "mounted path. Cloud credentials belong to the mount, not to this reader."
        )

    # local path, /Volumes/..., or an unknown non-cloud scheme -> pass through unchanged.
    return path


def index_candidates(path: str, suffixes) -> list:
    """Co-located index path candidates for ``path`` (e.g. ``.bai`` -> ``x.bam.bai``)."""
    return [path + s for s in suffixes]
