"""Pure-Python unit tests — no SparkSession/JVM required."""

import math
import os
import tempfile

import pytest

from conftest import resource

from litebfx import bgzf, io, regions
from litebfx.bed import parse_bed_line
from litebfx.fastq import read_number


def test_normalize_path():
    assert io.normalize_path("dbfs:/mnt/x/a.bam") == "/dbfs/mnt/x/a.bam"
    assert io.normalize_path("/Volumes/c/s/v/a.bam") == "/Volumes/c/s/v/a.bam"
    assert io.normalize_path("/local/a.bam") == "/local/a.bam"
    assert io.normalize_path("file:///data/a.bam") == "/data/a.bam"
    for uri in ("s3://b/a.bam", "s3a://b/a.bam", "abfss://c@x/a.bam", "gs://b/a.bam"):
        with pytest.raises(io.UnsupportedPathError):
            io.normalize_path(uri)


def test_parse_region():
    assert regions.parse_region("chr1:1,000-2,000") == regions.Region("chr1", 1000, 2000)
    assert regions.parse_region("chrX").fetch_args() == ("chrX", None, None)
    assert regions.parse_region("chr1:500").fetch_args() == ("chr1", 499, 500)
    assert regions.parse_region("") is None


def test_push_region():
    from litebfx.regions import Region, push_region

    def flt(name, attribute, value):        # duck-typed stand-in for a Spark 4.1 Filter
        return type(name, (), {"attribute": attribute, "value": value})()

    eq = flt("EqualTo", "referenceName", "chr1")
    ge = flt("GreaterThanOrEqual", "start", 1000)
    le = flt("LessThanOrEqual", "start", 2000)

    region, unhandled = push_region([eq, ge, le], "referenceName", "start")
    assert region == Region("chr1", 1000, 2000)
    assert len(unhandled) == 2              # ref-eq fully handled; ranges re-checked by Spark

    # a range without a contig equality -> no region, nothing pushed
    r2, u2 = push_region([ge], "referenceName", "start")
    assert r2 is None and len(u2) == 1


def test_read_number():
    assert read_number("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz") == 1
    assert read_number("sample.R2.fq") == 2
    assert read_number("reads_2.fastq") == 2
    assert read_number("reads.fastq") is None


def test_parse_bed_line():
    # clean BED3 / BED12; itemRgb comma-form passes through, block trailing comma stripped
    assert parse_bed_line("chr1\t5\t10") == ("chr1", 5, 10, None, None, None,
                                             None, None, None, None, None, None)
    assert parse_bed_line("chr2\t0\t100\tp\t500\t-\t0\t100\t255,0,0\t2\t10,20,\t0,80,") == \
        ("chr2", 0, 100, "p", 500, "-", 0, 100, "255,0,0", 2, "10,20", "0,80")

    # NO TRUNCATION: decimals in numeric columns -> null (score/thickStart/blockCount).
    # Shared conventions with the JAR: name/strand "." -> null, itemRgb "0" -> null.
    assert parse_bed_line("chr1\t0\t100\t.\t19.7\t.\t50.5\t-1\t0\t2.0\t1,2\t0,5") == \
        ("chr1", 0, 100, None, None, None, None, -1, None, None, "1,2", "0,5")

    # itemRgb packed integer -> "R,G,B"
    assert parse_bed_line("chr1\t0\t100\tn\t0\t+\t0\t100\t16711680") == \
        ("chr1", 0, 100, "n", 0, "+", 0, 100, "255,0,0", None, None, None)

    # malformed / too-short intervals are dropped (as samtools drops a bad region)
    assert parse_bed_line("chr1\t1.5\t100") is None
    assert parse_bed_line("chr1\t5") is None
    assert parse_bed_line("") is None


def _make_multiblock_bam(path, repeat=40):
    import pysam
    with pysam.AlignmentFile(resource("range.bam"), "rb") as s:
        hdr, recs = s.header, list(s.fetch(until_eof=True))
    with pysam.AlignmentFile(path, "wb", header=hdr) as o:
        for _ in range(repeat):
            for r in recs:
                o.write(r)
    return len(recs) * repeat


def test_bgzf_unindexed_split_lossless():
    """Production split_start_voffset: N-way byte split reproduces the full read (lossless)."""
    import pysam
    pysam = pytest.importorskip("pysam")
    tmp = tempfile.mkdtemp()
    big = os.path.join(tmp, "big.bam")
    n = _make_multiblock_bam(big)
    size = os.path.getsize(big)

    with pysam.AlignmentFile(big, "rb") as af:
        nref = af.nreferences
        full = [(r.query_name, r.reference_id, r.reference_start, r.flag)
                for r in af.fetch(until_eof=True)]

    def read_chunk(start, end):
        af = pysam.AlignmentFile(big, "rb")
        try:
            if start == 0:
                af.seek(af.tell())
            else:
                v = bgzf.split_start_voffset(big, start, nref)
                if v is None:
                    return []
                af.seek(v)
            out = []
            while True:
                if (af.tell() >> 16) >= end:
                    break
                try:
                    r = next(af)
                except StopIteration:
                    break
                out.append((r.query_name, r.reference_id, r.reference_start, r.flag))
            return out
        finally:
            af.close()

    nchunks, S = 8, math.ceil(size / 8)
    flat = []
    for i in range(nchunks):
        flat += read_chunk(i * S, (min((i + 1) * S, size) if i < nchunks - 1 else size + 1))

    assert len(flat) == n == len(full)
    assert flat == full            # order + content preserved, no gaps or duplicates
