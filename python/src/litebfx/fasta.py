"""FASTA DataSource — one row per contig, one partition per contig when a ``.fai`` exists."""

import os
from dataclasses import dataclass, field

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from ._base import (METADATA_FIELD, metadata_value, resolve_files, resolve_index,
                    wants_metadata)
from .arrow import batches, to_arrow_schema
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
            if fai and os.path.exists(fai):
                with open(fai) as fh:
                    contigs = [ln.split("\t", 1)[0] for ln in fh if ln.strip()]
                parts += ([_FastaPartition(path, [c], fai) for c in contigs]
                          or [_FastaPartition(path, [], fai)])
            else:
                parts.append(_FastaPartition(path, [], None))
        return parts

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        import pysam
        md = (metadata_value(partition.path, partition.index),) if self.metadata else ()
        if partition.contigs:
            with pysam.FastaFile(partition.path, filepath_index=partition.index) as fa:
                for contig in partition.contigs:
                    seq = fa.fetch(contig)
                    yield (contig, seq, len(seq)) + md
        else:
            with pysam.FastxFile(partition.path) as fx:
                for e in fx:
                    yield (e.name, e.sequence, len(e.sequence)) + md
