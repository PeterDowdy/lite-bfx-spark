"""Read parity — the DataSource output must match an independent oracle for each format.

Oracles are computed straight from pysam / the raw file (BAM/SAM tags come from htslib's own
SAM writer via ``AlignedSegment.to_string``, independent of litebfx's tag formatting), and
VCF is checked against hand-written expected values. Comparisons are multiset-based because
partition order is not guaranteed.
"""

import gzip

import pytest

from conftest import multiset, norm, resource

pytestmark = pytest.mark.spark


# --- FASTA ------------------------------------------------------------------------------
def test_fasta_parity(spark):
    import pysam
    fa = resource("realn01.fa")
    with pysam.FastaFile(fa) as f:
        exp = multiset([(c, f.fetch(c), f.get_reference_length(c)) for c in f.references])
    got = multiset(spark.read.format("fasta").load(fa).collect())
    assert got == exp


# --- FASTQ ------------------------------------------------------------------------------
def test_fastq_parity(spark):
    import pysam
    fq = resource("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz")
    with pysam.FastxFile(fq) as fx:
        exp = multiset([(e.name, e.sequence, e.quality, e.comment or None, 1) for e in fx])
    got = multiset(spark.read.format("fastq").load(fq).collect())
    assert got == exp
    assert all(norm(r)[4] == 1 for r in spark.read.format("fastq").load(fq).limit(5).collect())


# --- BED --------------------------------------------------------------------------------
def _bed_int(s):
    # strict integer, never truncate (narrowPeak floats -> null)
    try:
        return int(s)
    except ValueError:
        return None


def _bed_str(s):
    s = s.strip()
    return None if s in (".", "") else s


def _bed_rgb(s):
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


def _bed_block(s):
    s = s.strip()
    return None if not s else (s[:-1] if s.endswith(",") else s)


def _oracle_bed(path):
    rows = []
    with gzip.open(path, "rt") as fh:
        for line in fh:
            if not line.strip() or line.startswith(("#", "track", "browser")):
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) < 3:
                continue
            start, end = _bed_int(f[1]), _bed_int(f[2])
            if start is None or end is None:
                continue
            n = len(f)
            g = (lambda i: f[i] if i < n else None)
            rows.append((
                f[0], start, end,
                _bed_str(g(3)) if n > 3 else None,
                _bed_int(g(4)) if n > 4 else None,
                _bed_str(g(5)) if n > 5 else None,
                _bed_int(g(6)) if n > 6 else None,
                _bed_int(g(7)) if n > 7 else None,
                _bed_rgb(g(8)) if n > 8 else None,
                _bed_int(g(9)) if n > 9 else None,
                _bed_block(g(10)) if n > 10 else None,
                _bed_block(g(11)) if n > 11 else None,
            ))
    return rows


def test_bed_parity(spark):
    bed = resource("example.bed.gz")
    exp = multiset(_oracle_bed(bed))
    got = multiset(spark.read.format("bed").load(bed).collect())
    assert got == exp


# --- BAM / SAM --------------------------------------------------------------------------
def _oracle_aln(af):
    import pysam
    rows = []
    for r in af.fetch(until_eof=True):
        fields = r.to_string().split("\t")
        tags = {}
        for tok in fields[11:]:
            tags[tok[:2]] = tok[3:]                 # "NM:i:3" -> "NM": "i:3"
        rs, ms = r.reference_start, r.next_reference_start
        quals = (pysam.array_to_qualitystring(r.query_qualities)
                 if r.query_qualities is not None else None)
        rows.append((
            r.query_name, r.flag, r.reference_name,
            (rs + 1) if rs is not None and rs >= 0 else None,
            r.mapping_quality, r.cigarstring, r.next_reference_name,
            (ms + 1) if ms is not None and ms >= 0 else None,
            r.template_length, r.query_sequence, quals, tags,
            rs if rs is not None and rs >= 0 else None,
        ))
    return rows


def test_sam_parity(spark):
    import pysam
    sam = resource("realn02-r.sam")
    with pysam.AlignmentFile(sam, "r") as af:
        exp = multiset(_oracle_aln(af))
    got = multiset(spark.read.format("bam").load(sam).collect())
    assert got == exp


def test_bam_indexed_parity(spark):
    import pysam
    bam = resource("range.bam")
    with pysam.AlignmentFile(bam, "rb") as af:
        exp = multiset(_oracle_aln(af))
    got = multiset(spark.read.format("bam").load(bam).collect())   # per-contig + unmapped
    assert got == exp


def test_bam_region_option(spark):
    import pysam
    bam = resource("range.bam")
    with pysam.AlignmentFile(bam, "rb") as af:
        exp = {r.query_name for r in af.fetch("CHROMOSOME_I", 0, 2000)}
    got = {row.readName for row in
           spark.read.format("bam").option("region", "CHROMOSOME_I:1-2000").load(bam).collect()}
    assert got == exp


# --- VCF (explicit expected values — fully independent of litebfx) ----------------------
def test_vcf_parity(spark, vcf_fixture):
    rows = {r.pos: r for r in spark.read.format("vcf").load(vcf_fixture).collect()}
    r1 = rows[100]
    assert (r1.chrom, r1.id, r1.ref, list(r1.alt), r1.qual, r1.filter) == \
        ("chr1", "rs1", "A", ["T"], 50.0, "PASS")
    assert r1.info["DP"] == "10" and r1.info["AF"] == "0.5" and r1.info["DB"] == "true"
    assert r1.format == "GT:DP"
    assert r1.genotypes["S1"] == "0/1:9" and r1.genotypes["S2"] == "1|1:8"

    r2 = rows[200]
    assert r2.id is None and list(r2.alt) == ["C", "T"] and r2.qual is None
    assert r2.filter == "q10"
    assert r2.genotypes["S2"] == "./.:."
