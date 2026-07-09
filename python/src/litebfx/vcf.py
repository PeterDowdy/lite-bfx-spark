"""VCF / BCF DataSource. Full scan or tabix/CSI region query; BCF auto-detected by pysam."""

from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from ._base import (METADATA_FIELD, get_opt, metadata_value, resolve_files,
                    resolve_index, wants_metadata)
from .arrow import batches, to_arrow_schema
from .regions import parse_region, push_region
from .schemas import VCF_SCHEMA

_EXT = (".vcf", ".vcf.gz", ".vcf.bgz", ".bcf")


def _fmt_value(v):
    if v is None:
        return "."
    if isinstance(v, (tuple, list)):
        return ",".join("." if x is None else str(x) for x in v)
    return str(v)


def _fmt_info(info):
    out = {}
    for k, v in info.items():
        out[k] = "true" if v is True else _fmt_value(v)
    return out


def _fmt_gt(sample):
    alleles = sample.get("GT")
    if alleles is None:
        return "."
    sep = "|" if sample.phased else "/"
    return sep.join("." if a is None else str(a) for a in alleles)


def _genotypes(rec, fmt_keys):
    out = {}
    for name, sample in rec.samples.items():
        parts = []
        for k in fmt_keys:
            parts.append(_fmt_gt(sample) if k == "GT" else _fmt_value(sample.get(k)))
        out[name] = ":".join(parts)
    return out


def record_to_row(rec):
    fmt_keys = list(rec.format.keys())
    filt = list(rec.filter.keys())
    return (
        rec.chrom,
        rec.pos,                                            # 1-based
        rec.id,
        rec.ref,
        list(rec.alts) if rec.alts else None,
        rec.qual,
        ";".join(filt) if filt else None,
        _fmt_info(rec.info),
        ":".join(fmt_keys) if fmt_keys else None,
        _genotypes(rec, fmt_keys) if fmt_keys else None,
    )


@dataclass
class _VcfPartition(InputPartition):
    path: str = ""
    index: str = None
    region: tuple = None


class VcfDataSource(DataSource):
    # Set by register_all() -> _VcfDataSourcePushdown when the Spark session has opted in to
    # spark.sql.python.filterPushdown.enabled; see that class and __init__.py. Spark errors
    # out at plan time if pushFilters() is implemented but that conf is false, so the reader
    # class itself (not just its behavior) must vary on this flag.
    _pushdown_enabled = False

    @classmethod
    def name(cls):
        return "vcf"

    def schema(self):
        if wants_metadata(self.options):
            return StructType(list(VCF_SCHEMA.fields) + [METADATA_FIELD])
        return VCF_SCHEMA

    def reader(self, schema):
        cls = _VcfReaderPushdown if self._pushdown_enabled else _VcfReader
        return cls(self.options, schema)


class _VcfDataSourcePushdown(VcfDataSource):
    _pushdown_enabled = True


class _VcfReader(DataSourceReader):
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
            region = self.region.fetch_args() if (idx and self.region) else None
            parts.append(_VcfPartition(path, idx, region))
        return parts

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        import pysam
        md = (metadata_value(partition.path, partition.index),) if self.metadata else ()
        vf = pysam.VariantFile(partition.path, index_filename=partition.index)
        try:
            it = vf.fetch(*partition.region) if partition.region else vf
            for rec in it:
                yield record_to_row(rec) + md
        finally:
            vf.close()


class _VcfReaderPushdown(_VcfReader):
    def pushFilters(self, filters):
        if not all(self._indexes.values()) or self.region is not None:
            return filters
        region, unhandled = push_region(filters, "chrom", "pos")
        if region is not None:
            self.region = region
        return unhandled
