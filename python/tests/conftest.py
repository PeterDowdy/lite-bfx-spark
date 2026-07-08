"""Shared test fixtures. Reuses the JAR's checked-in fixtures under core/src/test/resources."""

import collections
import os

import pytest

_HERE = os.path.dirname(__file__)
_RES = os.path.abspath(os.path.join(_HERE, "..", "..", "core", "src", "test", "resources"))


def resource(name):
    return os.path.join(_RES, name)


def norm(row):
    """Normalize a Spark Row / tuple to a hashable, order-stable form for multiset compares."""
    vals = list(row)
    out = []
    for v in vals:
        if isinstance(v, dict):
            out.append(tuple(sorted(v.items())))
        elif isinstance(v, (list, tuple)):
            out.append(tuple(v))
        else:
            out.append(v)
    return tuple(out)


def multiset(rows):
    return collections.Counter(norm(r) for r in rows)


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("litebfx-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    import litebfx
    litebfx.register_all(s)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def vcf_fixture(tmp_path_factory):
    """A tiny bgzipped + tabix-indexed VCF (no VCF fixture is checked into the repo)."""
    import pysam
    d = tmp_path_factory.mktemp("vcf")
    plain = str(d / "calls.vcf")
    with open(plain, "w") as fh:
        fh.write(
            "##fileformat=VCFv4.2\n"
            '##FILTER=<ID=PASS,Description="passed">\n'
            '##FILTER=<ID=q10,Description="low qual">\n'
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="depth">\n'
            '##INFO=<ID=AF,Number=A,Type=Float,Description="af">\n'
            '##INFO=<ID=DB,Number=0,Type=Flag,Description="dbsnp">\n'
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
            '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="dp">\n'
            "##contig=<ID=chr1>\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n"
            "chr1\t100\trs1\tA\tT\t50\tPASS\tDP=10;AF=0.5;DB\tGT:DP\t0/1:9\t1|1:8\n"
            "chr1\t200\t.\tG\tC,T\t.\tq10\tDP=3\tGT:DP\t0/0:2\t./.:.\n"
        )
    bgz = plain + ".gz"
    pysam.tabix_compress(plain, bgz, force=True)
    pysam.tabix_index(bgz, preset="vcf", force=True)
    return bgz
