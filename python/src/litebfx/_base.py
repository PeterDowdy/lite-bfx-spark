"""Shared plumbing for the format DataSources."""

import datetime as _dt
import glob
import json
import os

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from . import _cloudfs
from .io import is_cloud_path, normalize_path

_INDEX_EXTS = (".bai", ".crai", ".csi", ".tbi", ".fai", ".gzi")


def get_opt(options, key, default=None):
    """Case-insensitive option lookup (Spark lowercases option keys, but be defensive)."""
    key = key.lower()
    for k, v in (options or {}).items():
        if k.lower() == key:
            return v
    return default


def get_path(options):
    """The single normalized local path passed to ``.load(...)`` (first of resolve_files)."""
    return resolve_files(options)[0]


def _raw_paths(options):
    paths = []
    multi = get_opt(options, "paths")            # Spark sets this for .load(a, b, ...)
    if multi:
        try:
            paths.extend(json.loads(multi))
        except (ValueError, TypeError):
            paths.append(multi)
    single = get_opt(options, "path")
    if single and single not in paths:
        paths.append(single)
    return paths


def resolve_files(options, extensions=None):
    """Expand path/paths options — globs and directories — to a sorted list of local files.

    A glob pattern is expanded; a directory yields its files matching ``extensions`` (index
    files excluded); anything else is taken as a single file. ``extensions`` is required to
    list a directory (each format passes its own).
    """
    out = []
    for raw in _raw_paths(options):
        path = normalize_path(raw)
        if any(c in path for c in "*?["):
            if is_cloud_path(path):
                raise ValueError(
                    f"litebfx: glob patterns are not supported on cloud paths ({path!r}). "
                    "Pass an explicit multi-path .load(a, b, ...) or a plain directory path."
                )
            out.extend(sorted(glob.glob(path)))
        elif _cloudfs.isdir(path):
            if not extensions:
                raise ValueError(f"litebfx: {path!r} is a directory; this format cannot list it")
            for name in sorted(_cloudfs.listdir(path)):
                if name.endswith(extensions) and not name.endswith(_INDEX_EXTS):
                    out.append(os.path.join(path, name))
        else:
            out.append(path)
    files, seen = [], set()
    for f in out:                                # de-dup, keep order
        if f not in seen:
            seen.add(f)
            files.append(f)
    if not files:
        raise ValueError("litebfx: no input files matched the given path(s)")
    return files


def resolve_index(options, path, suffixes, single_file):
    """Resolve an index for ``path``: indexPath (single-file only) -> indexDir -> co-located."""
    ip = get_opt(options, "indexpath")
    if ip and single_file:
        return normalize_path(ip)
    idir = get_opt(options, "indexdir")
    if idir:
        idir = normalize_path(idir)
        base = os.path.basename(path)
        for s in suffixes:
            cand = os.path.join(idir, base + s)
            if _cloudfs.exists(cand):
                return cand
    return existing_index(path, suffixes)


# --- opt-in _metadata column (visible; the Python API cannot add hidden columns) ----------

METADATA_FIELD = StructField("_metadata", StructType([
    StructField("file_path", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("file_modification_time", TimestampType(), True),
    StructField("index_path", StringType(), True),
]), True)


def wants_metadata(options):
    return str(get_opt(options, "metadata", "false")).lower() == "true"


def metadata_value(path, index_path=None):
    size, mtime = _cloudfs.stat(path)
    return (path, os.path.basename(path), size,
            _dt.datetime.fromtimestamp(mtime) if mtime is not None else None, index_path)


def num_partitions(options, default=200):
    v = get_opt(options, "numpartitions")
    return int(v) if v else default


def existing_index(path, suffixes):
    """First co-located index that exists (local or cloud), or None."""
    for s in suffixes:
        cand = path + s
        if _cloudfs.exists(cand):
            return cand
    return None


def import_pysam():
    """Import pysam, working around a Databricks Runtime crash.

    Databricks Runtime bakes OPENSSL_FORCE_FIPS_MODE (e.g. "0") into the process
    environment for its own bundled OpenSSL. pysam's wheel vendors its own OpenSSL 1.1.1
    build, and merely seeing that var defined — regardless of value — makes it run a FIPS
    self-test that fails on the auditwheel-relocated binary and aborts the process (SIGABRT).
    Only the var's absence avoids the self-test, so pop it before the first import.
    """
    os.environ.pop("OPENSSL_FORCE_FIPS_MODE", None)
    import pysam
    return pysam


def round_robin(items, n):
    """Split ``items`` into at most ``n`` contiguous-ish groups (non-empty)."""
    if n <= 1 or len(items) <= 1:
        return [list(items)] if items else []
    n = min(n, len(items))
    k, r = divmod(len(items), n)
    groups, i = [], 0
    for g in range(n):
        size = k + (1 if g < r else 0)
        groups.append(list(items[i:i + size]))
        i += size
    return groups
