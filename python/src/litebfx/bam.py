"""BAM / SAM / CRAM DataSource.

Per file, partition planning mirrors the JAR (adapted to pysam):

* SAM (text)                      -> single streaming partition
* BAM + BAI, region pushed        -> single ``fetch(contig, start, end)``
* BAM + BAI, no region            -> one partition per contig group + one unmapped
* BAM, no index                   -> BGZF byte-range splits (block + record guesser)
* CRAM + CRAI                     -> region query / per-contig groups
* CRAM, no index                  -> driver synthesizes a CRAI (sorted, writable dir) then
                                     per-contig; else a single partition

Multiple files / globs / directories each contribute their own partitions. Record decoding
stays entirely in pysam, so BAM/SAM/CRAM share one mapping and parity holds.
"""

import math
import os
from dataclasses import dataclass, field

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from ._base import (METADATA_FIELD, get_opt, metadata_value, num_partitions,
                    resolve_files, resolve_index, round_robin, wants_metadata)
from .arrow import batches, to_arrow_schema
from .bgzf import split_start_voffset
from .io import normalize_path
from .regions import parse_region, push_region
from .schemas import bam_schema, is_sam_column_names

_DEFAULT_SPLIT = 128 * 1024 * 1024   # bgzfSplitSize default, matches the JAR


# --- record -> row ----------------------------------------------------------------------

def _quals(r):
    import pysam
    q = r.query_qualities
    return None if q is None else pysam.array_to_qualitystring(q)


def _fmt_tag(val, typ):
    if typ in "cCsSiI":                       # SAM writes every integer type as 'i'
        return f"i:{val}"
    if typ == "f":
        return f"f:{val}"
    if typ in "AZH":
        return f"{typ}:{val}"
    if isinstance(val, (list, tuple)) or hasattr(val, "typecode"):
        sub = getattr(val, "typecode", "i")
        sub = "i" if sub in "bBhHiIlLqQ" else ("f" if sub in "fd" else sub)
        return "B:" + sub + "," + ",".join(str(x) for x in val)
    return f"{typ}:{val}"


def _tags(r):
    return {tag: _fmt_tag(val, typ) for tag, val, typ in r.get_tags(with_value_type=True)}


def record_to_row(r):
    rstart = r.reference_start           # 0-based, -1 when unmapped
    mstart = r.next_reference_start
    return (
        r.query_name,
        r.flag,
        r.reference_name,
        (rstart + 1) if rstart is not None and rstart >= 0 else None,   # 1-based start
        r.mapping_quality,
        r.cigarstring,
        r.next_reference_name,
        (mstart + 1) if mstart is not None and mstart >= 0 else None,   # 1-based mateStart
        r.template_length,
        r.query_sequence,
        _quals(r),
        _tags(r),
        rstart if rstart is not None and rstart >= 0 else None,          # 0-based start0
    )


# --- partitions -------------------------------------------------------------------------

@dataclass
class _AlnPartition(InputPartition):
    kind: str = "all"                          # all | contigs | region | unmapped | range
    path: str = ""
    index: str = None
    contigs: list = field(default_factory=list)
    region: tuple = None                       # (contig, start0, end)
    start_byte: int = 0
    end_byte: int = 0


# --- reader -----------------------------------------------------------------------------

class _AlignmentReader(DataSourceReader):
    def __init__(self, options, schema, is_cram=False):
        self.options = options
        self.is_cram = is_cram
        self._arrow = to_arrow_schema(schema)
        self.files = resolve_files(options, (".cram",) if is_cram else (".bam", ".sam"))
        self.single = len(self.files) == 1
        self.sam_names = is_sam_column_names(options)
        self.num_partitions = num_partitions(options)
        self.split_size = int(get_opt(options, "bgzfsplitsize", _DEFAULT_SPLIT))
        self.use_index = str(get_opt(options, "useindex", "true")).lower() != "false"
        self.metadata = wants_metadata(options)
        ref = get_opt(options, "referencefile")
        self.reference = normalize_path(ref) if ref else None
        r = get_opt(options, "region")
        self.region = parse_region(r) if r else None
        self._ref_col = "rname" if self.sam_names else "referenceName"
        self._coord_col = "pos" if self.sam_names else "start"
        suffixes = (".crai",) if is_cram else (".bai", ".csi")
        self._indexes = {p: (resolve_index(options, p, suffixes, self.single)
                             if self.use_index else None) for p in self.files}

    def _open(self, path, index=None):
        import pysam
        if (not self.is_cram) and path.lower().endswith(".sam"):
            return pysam.AlignmentFile(path, "r")
        kw = {}
        if self.reference:
            kw["reference_filename"] = self.reference
        if index:
            kw["index_filename"] = index
        return pysam.AlignmentFile(path, "rc" if self.is_cram else "rb", **kw)

    def _synthesize_crai(self, path):
        """Build a co-located CRAI for an unindexed (coordinate-sorted) CRAM. None if the CRAM
        is unsorted or its directory is read-only."""
        import pysam
        crai = path + ".crai"
        if os.path.exists(crai):
            return crai
        try:
            pysam.index(path)
            return crai if os.path.exists(crai) else None
        except Exception:
            return None

    def partitions(self):
        parts = []
        for path in self.files:
            parts += self._file_partitions(path)
        return parts

    def _file_partitions(self, path):
        if (not self.is_cram) and path.lower().endswith(".sam"):
            return [_AlnPartition("all", path)]

        index = self._indexes.get(path)
        if index and self.region:
            return [_AlnPartition("region", path, index, region=self.region.fetch_args())]
        if not index and self.is_cram:
            index = self._synthesize_crai(path)

        if index:
            with self._open(path, index) as af:
                refs = list(af.references)
            parts = [_AlnPartition("contigs", path, index, contigs=g)
                     for g in round_robin(refs, self.num_partitions)]
            parts.append(_AlnPartition("unmapped", path, index))
            return parts or [_AlnPartition("all", path)]

        if self.is_cram:                       # unindexed CRAM, could not synthesize an index
            return [_AlnPartition("all", path)]

        size = os.path.getsize(path)           # unindexed BAM -> BGZF byte-range splits
        n = max(1, math.ceil(size / self.split_size))
        return [_AlnPartition("range", path, start_byte=i * self.split_size,
                              end_byte=min((i + 1) * self.split_size, size))
                for i in range(n)]

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        md = (metadata_value(partition.path, partition.index),) if self.metadata else ()
        af = self._open(partition.path, partition.index)
        try:
            if partition.kind == "all":
                for r in af.fetch(until_eof=True):
                    yield record_to_row(r) + md
            elif partition.kind == "region":
                for r in af.fetch(*partition.region):
                    yield record_to_row(r) + md
            elif partition.kind == "contigs":
                for contig in partition.contigs:
                    for r in af.fetch(contig):
                        yield record_to_row(r) + md
            elif partition.kind == "unmapped":
                for r in af.fetch(contig="*"):
                    yield record_to_row(r) + md
            elif partition.kind == "range":
                for row in self._read_range(af, partition.path,
                                            partition.start_byte, partition.end_byte):
                    yield row + md
        finally:
            af.close()

    def _read_range(self, af, path, start_byte, end_byte):
        """Unindexed BAM byte-range split: block+record guesser -> seek -> stop at boundary."""
        if start_byte == 0:
            af.seek(af.tell())
        else:
            v = split_start_voffset(path, start_byte, af.nreferences)
            if v is None:
                return
            af.seek(v)
        while True:
            vt = af.tell()
            if (vt >> 16) >= end_byte:
                break
            try:
                r = next(af)
            except StopIteration:
                break
            yield record_to_row(r)


class _AlignmentReaderPushdown(_AlignmentReader):
    def pushFilters(self, filters):
        """Spark 4.1+ pushdown; only prune when every file has an index that can enforce it."""
        if not all(self._indexes.values()) or self.region is not None:
            return filters
        region, unhandled = push_region(filters, self._ref_col, self._coord_col)
        if region is not None:
            self.region = region
        return unhandled


class BamDataSource(DataSource):
    # Set by register_all() -> _BamDataSourcePushdown when the Spark session has opted in
    # to spark.sql.python.filterPushdown.enabled; see that class and __init__.py. Spark
    # errors out at plan time if pushFilters() is implemented but that conf is false, so the
    # reader class itself (not just its behavior) must vary on this flag.
    _pushdown_enabled = False

    @classmethod
    def name(cls):
        return "bam"

    def schema(self):
        base = bam_schema(is_sam_column_names(self.options))
        if wants_metadata(self.options):
            return StructType(list(base.fields) + [METADATA_FIELD])
        return base

    def reader(self, schema):
        cls = _AlignmentReaderPushdown if self._pushdown_enabled else _AlignmentReader
        return cls(self.options, schema, is_cram=False)


class _BamDataSourcePushdown(BamDataSource):
    _pushdown_enabled = True


class CramDataSource(DataSource):
    _pushdown_enabled = False   # see BamDataSource

    @classmethod
    def name(cls):
        return "cram"

    def schema(self):
        base = bam_schema(is_sam_column_names(self.options))
        if wants_metadata(self.options):
            return StructType(list(base.fields) + [METADATA_FIELD])
        return base

    def reader(self, schema):
        cls = _AlignmentReaderPushdown if self._pushdown_enabled else _AlignmentReader
        return cls(self.options, schema, is_cram=True)


class _CramDataSourcePushdown(CramDataSource):
    _pushdown_enabled = True
