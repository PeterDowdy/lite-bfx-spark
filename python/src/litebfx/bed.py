"""BED DataSource — BED3..BED12, plain or bgzipped, optional tabix region query."""

import gzip
from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from ._base import (METADATA_FIELD, get_opt, import_pysam, metadata_value, resolve_files,
                    resolve_index, wants_metadata)
from .arrow import batches, to_arrow_schema
from .regions import parse_region, push_region
from .schemas import BED_SCHEMA

_EXT = (".bed", ".bed.gz", ".bed.bgz")

# BED is an interval format: chrom/chromStart/chromEnd are parsed strictly (a non-integer
# coordinate drops the record, as samtools drops a malformed region). Numeric columns take a
# clean integer or null — **never truncated** (samtools does not truncate BED numbers). The
# remaining conventions (absent-marker "."/"0" -> null, packed-int itemRgb -> "R,G,B",
# trailing-comma stripping on block fields) match the Java reader so the two stay identical;
# see python/README.md "BED number handling".


def _int(s):
    """Strict integer parse; None for non-integers such as ``19.76368`` / ``818.0`` (no
    truncation — samtools never truncates a BED numeric column)."""
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _str(s):
    """name/strand: a bare ``.`` or empty means absent -> null."""
    s = s.strip()
    return None if s in (".", "") else s


def _item_rgb(s):
    """itemRgb: ``0``/empty -> null; ``R,G,B`` passes through; a packed integer -> ``R,G,B``."""
    s = s.strip()
    if s in ("", "0"):
        return None
    if "," in s:
        return s
    try:
        v = int(s)
    except ValueError:
        return s
    return f"{(v >> 16) & 0xFF},{(v >> 8) & 0xFF},{v & 0xFF}"


def _block(s):
    """blockSizes/blockStarts: empty -> null; strip a single trailing comma."""
    s = s.strip()
    if not s:
        return None
    return s[:-1] if s.endswith(",") else s


def parse_bed_line(line):
    """Parse one BED line into the 12-column tuple, or None to drop the record."""
    f = line.rstrip("\n").split("\t")
    if len(f) < 3 or not f[0]:
        return None
    start, end = _int(f[1]), _int(f[2])
    if start is None or end is None:
        return None
    n = len(f)

    def col(i):
        return f[i] if i < n else None

    return (
        f[0], start, end,
        _str(col(3)) if n > 3 else None,
        _int(col(4)) if n > 4 else None,
        _str(col(5)) if n > 5 else None,
        _int(col(6)) if n > 6 else None,
        _int(col(7)) if n > 7 else None,
        _item_rgb(col(8)) if n > 8 else None,
        _int(col(9)) if n > 9 else None,
        _block(col(10)) if n > 10 else None,
        _block(col(11)) if n > 11 else None,
    )


def _is_data_line(line):
    return line and not line.startswith(("#", "track", "browser"))


@dataclass
class _BedPartition(InputPartition):
    path: str = ""
    index: str = None
    region: tuple = None      # (contig, start0, end) for a tabix query, else None


class BedDataSource(DataSource):
    # Set by register_all() -> _BedDataSourcePushdown when the Spark session has opted in to
    # spark.sql.python.filterPushdown.enabled; see that class and __init__.py. Spark errors
    # out at plan time if pushFilters() is implemented but that conf is false, so the reader
    # class itself (not just its behavior) must vary on this flag.
    _pushdown_enabled = False

    @classmethod
    def name(cls):
        return "bed"

    def schema(self):
        if wants_metadata(self.options):
            return StructType(list(BED_SCHEMA.fields) + [METADATA_FIELD])
        return BED_SCHEMA

    def reader(self, schema):
        cls = _BedReaderPushdown if self._pushdown_enabled else _BedReader
        return cls(self.options, schema)


class _BedDataSourcePushdown(BedDataSource):
    _pushdown_enabled = True


class _BedReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.files = resolve_files(options, _EXT)
        self.single = len(self.files) == 1
        self.metadata = wants_metadata(options)
        r = get_opt(options, "region")
        self.region = parse_region(r) if r else None
        self._indexes = {p: resolve_index(options, p, (".tbi", ".csi"), self.single)
                         for p in self.files}
        self._arrow = to_arrow_schema(schema)

    def partitions(self):
        parts = []
        for path in self.files:
            idx = self._indexes[path]
            if idx and self.region:
                parts.append(_BedPartition(path, idx, self.region.fetch_args()))
            else:
                parts.append(_BedPartition(path, idx, None))
        return parts

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        md = (metadata_value(partition.path, partition.index),) if self.metadata else ()
        if partition.region:
            pysam = import_pysam()
            c, s0, e = partition.region
            with pysam.TabixFile(partition.path, index=partition.index) as tbx:
                for line in tbx.fetch(c, s0, e):
                    row = parse_bed_line(line)
                    if row:
                        yield row + md
        else:
            for line in _open_text(partition.path):
                if _is_data_line(line):
                    row = parse_bed_line(line)
                    if row:
                        yield row + md


class _BedReaderPushdown(_BedReader):
    def pushFilters(self, filters):
        # Only prune when every file has an index to enforce it; else Spark filters post-scan.
        if not all(self._indexes.values()) or self.region is not None:
            return filters
        region, unhandled = push_region(filters, "chrom", "chromStart")
        if region is not None:
            self.region = region
        return unhandled


def _open_text(path):
    """Iterate text lines of a plain or gzip/bgzip BED file."""
    if path.endswith((".gz", ".bgz")):
        with gzip.open(path, "rt") as fh:
            yield from fh
    else:
        with open(path) as fh:
            yield from fh
