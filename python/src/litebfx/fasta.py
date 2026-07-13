"""FASTA DataSource — one row per contig, one partition per contig when a ``.fai`` exists."""

import io
from dataclasses import dataclass, field

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from . import _cloud, _cloudfs
from ._base import (METADATA_FIELD, import_pysam, metadata_value, resolve_files, resolve_index,
                    wants_metadata)
from .arrow import batches, to_arrow_schema
from .io import cloud_read_mode
from .schemas import FASTA_SCHEMA

_EXT = (".fa", ".fasta", ".fna", ".fa.gz", ".fasta.gz")


@dataclass
class _FastaPartition(InputPartition):
    path: str = ""
    contigs: list = field(default_factory=list)   # empty => stream the whole file
    index: str = None


class FastaDataSource(DataSource):
    @classmethod
    def name(cls):
        return "fasta"

    def schema(self):
        if wants_metadata(self.options):
            return StructType(list(FASTA_SCHEMA.fields) + [METADATA_FIELD])
        return FASTA_SCHEMA

    def reader(self, schema):
        return _FastaReader(self.options, schema)


class _FastaReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.files = resolve_files(options, _EXT)
        self.single = len(self.files) == 1
        self.metadata = wants_metadata(options)
        self._arrow = to_arrow_schema(schema)

    def partitions(self):
        parts = []
        for path in self.files:
            fai = resolve_index(self.options, path, (".fai",), self.single)
            if fai and _cloudfs.exists(fai):
                with io.TextIOWrapper(_cloudfs.open_stream(fai)) as fh:
                    contigs = [ln.split("\t", 1)[0] for ln in fh if ln.strip()]
                parts += ([_FastaPartition(path, [c], fai) for c in contigs]
                          or [_FastaPartition(path, [], fai)])
            else:
                parts.append(_FastaPartition(path, [], None))
        return parts

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        md = (metadata_value(partition.path, partition.index),) if self.metadata else ()
        with _cloud.cloud_read_scope(partition.path):
            pysam = import_pysam()
            mode = cloud_read_mode(partition.path)
            path = _cloudfs.resolve_open_path(partition.path)
            if partition.contigs:
                if mode == "native":
                    # pysam.FastaFile's explicit filepath_index= does its own local-only
                    # os.path-style pre-check in Cython before ever reaching htslib -- it
                    # always reports "does not exist" for a remote URL regardless of correct
                    # syntax (confirmed empirically). Omitting it lets htslib auto-discover a
                    # co-located .fai itself (a real, remote-aware open), which works. A
                    # non-co-located index (indexPath/indexDir pointing elsewhere) on a
                    # native cloud path can't use this workaround -- passing it explicit
                    # deliberately raises pysam's local-only error rather than silently
                    # reading the wrong (or no) index; this is a known pysam limitation, not
                    # something litebfx's own code can route around.
                    co_located = partition.index == partition.path + ".fai"
                    kw = {} if (co_located or not partition.index) else (
                        {"filepath_index": _cloudfs.resolve_open_path(partition.index)})
                else:
                    # Local, or Azure "download" mode: materialize_local() already produced
                    # a real local file, so pysam's pre-check succeeds normally -- no
                    # co-located restriction needed, unlike the native-mode branch above.
                    kw = ({"filepath_index": _cloudfs.resolve_open_path(partition.index)}
                          if partition.index else {})
                with pysam.FastaFile(path, **kw) as fa:
                    for contig in partition.contigs:
                        seq = fa.fetch(contig)
                        yield (contig, seq, len(seq)) + md
            else:
                with pysam.FastxFile(path) as fx:
                    for e in fx:
                        yield (e.name, e.sequence, len(e.sequence)) + md
