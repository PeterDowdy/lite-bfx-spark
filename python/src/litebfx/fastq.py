"""FASTQ DataSource.

Multi-file/glob/directory reads; uncompressed FASTQ is split into byte ranges (each executor
resyncs to the next 4-line record). ``.gz`` is a single partition per file (plain gzip is not
seekable; BGZF ``.fastq.gz`` splitting is deferred). ``readNumber`` comes from the filename.
"""

import os
import re
from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from . import _cloud, _cloudfs
from ._base import (METADATA_FIELD, attach_credential, credential_for_partitions, get_opt,
                    import_pysam, metadata_value, num_partitions, resolve_files, wants_metadata)
from .arrow import batches, to_arrow_schema
from .schemas import FASTQ_SCHEMA

_EXT = (".fastq", ".fq", ".fastq.gz", ".fq.gz")
_R = re.compile(r"[._](?:R)?([12])[._]")     # ..._R1_...  / sample.R2.fq / reads_2.fastq
_DEFAULT_MIN_SPLIT = 64 * 1024 * 1024


def read_number(path):
    m = _R.search("_" + os.path.basename(path))
    return int(m.group(1)) if m else None


@dataclass
class _FastqPartition(InputPartition):
    path: str = ""
    start: int = 0
    end: int = 0
    whole: bool = True          # True => stream the whole file with pysam
    aws_credential: object = None              # driver-vended _cloud._DatabricksPathCredential;
                                                # see _base.attach_credential()


class FastqDataSource(DataSource):
    @classmethod
    def name(cls):
        return "fastq"

    def schema(self):
        if wants_metadata(self.options):
            return StructType(list(FASTQ_SCHEMA.fields) + [METADATA_FIELD])
        return FASTQ_SCHEMA

    def reader(self, schema):
        return _FastqReader(self.options, schema)


class _FastqReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.files = resolve_files(options, _EXT)
        self.metadata = wants_metadata(options)
        self.min_split = int(get_opt(options, "minsplitbytes", _DEFAULT_MIN_SPLIT))
        self.num_partitions = num_partitions(options)
        self._arrow = to_arrow_schema(schema)

    def partitions(self):
        parts = []
        for path in self.files:
            cred = credential_for_partitions(path)
            file_parts = []
            if path.endswith(".gz"):
                file_parts.append(_FastqPartition(path, 0, 0, True))
            else:
                size = _cloudfs.getsize(path)
                n = min(self.num_partitions, max(1, size // self.min_split))
                if n <= 1:
                    file_parts.append(_FastqPartition(path, 0, 0, True))
                else:
                    chunk = -(-size // n)                       # ceil division
                    for i in range(n):
                        s, e = i * chunk, min((i + 1) * chunk, size)
                        if s < size:
                            file_parts.append(_FastqPartition(path, s, e, False))
            parts += attach_credential(file_parts, cred)
        return parts

    def read(self, partition):
        yield from batches(self._rows(partition), self._arrow)

    def _rows(self, partition):
        rn = read_number(partition.path)
        md = (metadata_value(partition.path),) if self.metadata else ()
        with _cloud.cloud_read_scope(partition.path, partition.aws_credential):
            if partition.whole:
                pysam = import_pysam()
                with pysam.FastxFile(_cloudfs.resolve_open_path(partition.path)) as fx:
                    for e in fx:
                        yield (e.name, e.sequence, e.quality, e.comment or None, rn) + md
            else:
                yield from self._read_range(partition, rn, md)

    def _read_range(self, partition, rn, md):
        with _cloudfs.open_stream(partition.path) as f:
            if partition.start > 0:
                f.seek(partition.start)
                if not _resync(f):
                    return                                # no record start in this chunk
            while f.tell() < partition.end:
                h = f.readline()
                if not h:
                    break
                seq, _plus, qual = f.readline(), f.readline(), f.readline()
                if not qual:
                    break
                name, _, desc = h[1:].rstrip(b"\r\n").decode().partition(" ")
                yield (name, seq.rstrip(b"\r\n").decode(), qual.rstrip(b"\r\n").decode(),
                       desc or None, rn) + md


def _resync(f):
    """Position f at the next full FASTQ record: a ``@`` line whose 3rd following line is ``+``
    (disambiguates a quality line that happens to start with ``@``)."""
    while True:
        pos = f.tell()
        line = f.readline()
        if not line:
            return False
        if line[:1] == b"@":
            after = f.tell()
            f.readline()                                  # seq
            plus = f.readline()                           # +
            if plus[:1] == b"+":
                f.seek(pos)
                return True
            f.seek(after)                                 # false header -> keep scanning
