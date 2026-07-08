"""samtools parity — the BAM/SAM reader must match ``samtools view``.

``samtools view`` is the reference implementation and an entirely independent code path
from the reader's pysam bindings, so this is the strongest available value-parity oracle:
the canonical SAM text is parsed back into the reader's schema and compared as a multiset
(partition order is not guaranteed).

SAM-text → schema mapping mirrors the SAM spec: ``*`` → null (rname/cigar/seq/qual),
``=`` → the read's own rname (rnext), ``0`` position → null start/mateStart, and each
``TAG:TYPE:VALUE`` optional field → ``TAG`` -> ``TYPE:VALUE`` in the attributes map.
"""

import shutil
import subprocess

import pytest

from conftest import multiset, resource

pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(shutil.which("samtools") is None, reason="samtools CLI not installed"),
]


def _sam_line_to_row(line):
    f = line.rstrip("\n").split("\t")
    rname = None if f[2] == "*" else f[2]
    pos = int(f[3])
    rnext = f[6]
    mate_rname = rname if rnext == "=" else (None if rnext == "*" else rnext)
    pnext = int(f[7])
    tags = {}
    for tok in f[11:]:
        if len(tok) >= 5 and tok[2] == ":":
            tags[tok[:2]] = tok[3:]                    # "NM:i:3" -> "NM": "i:3"
    return (
        f[0],                                          # readName
        int(f[1]),                                     # flags
        rname,                                         # referenceName
        pos if pos > 0 else None,                      # start (1-based; 0 => unmapped)
        int(f[4]),                                     # mappingQuality
        None if f[5] == "*" else f[5],                 # cigar
        mate_rname,                                    # mateReferenceName
        pnext if pnext > 0 else None,                  # mateStart
        int(f[8]),                                     # insertSize
        None if f[9] == "*" else f[9],                 # sequence
        None if f[10] == "*" else f[10],               # baseQualities
        tags,                                          # attributes
        (pos - 1) if pos > 0 else None,                # start0 (0-based)
    )


def _samtools_rows(path):
    out = subprocess.run(["samtools", "view", path],
                         capture_output=True, text=True, check=True).stdout
    return [_sam_line_to_row(ln) for ln in out.splitlines() if ln]


def test_bam_matches_samtools_view(spark):
    bam = resource("range.bam")
    exp = multiset(_samtools_rows(bam))                # indexed read = per-contig + unmapped
    got = multiset(spark.read.format("bam").load(bam).collect())
    assert got == exp


def test_sam_matches_samtools_view(spark):
    sam = resource("realn02-r.sam")
    exp = multiset(_samtools_rows(sam))
    got = multiset(spark.read.format("bam").load(sam).collect())
    assert got == exp


def test_bam_region_matches_samtools_view(spark):
    bam = resource("range.bam")
    out = subprocess.run(["samtools", "view", bam, "CHROMOSOME_I:1-2000"],
                         capture_output=True, text=True, check=True).stdout
    exp = multiset(_sam_line_to_row(ln) for ln in out.splitlines() if ln)
    got = multiset(
        spark.read.format("bam").option("region", "CHROMOSOME_I:1-2000").load(bam).collect()
    )
    assert got == exp
