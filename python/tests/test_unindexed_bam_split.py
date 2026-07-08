"""Unindexed BAM byte-range split — **losslessness only, NOT parity**.

The Python reader splits an unindexed BAM into arithmetic compressed-byte chunks and
resyncs with the BGZF block finder + record guesser. That produces different partition
*boundaries* than the JAR's byte-chunks, so no per-partition parity is asserted. What must
hold is that the union of all partitions equals a single full read exactly — every record
once, no gaps, no duplicates — regardless of how many chunks it is split into.
"""

import pytest

from conftest import multiset, resource

pytestmark = pytest.mark.spark


def _make_multiblock_bam(path, repeat=40):
    import pysam
    with pysam.AlignmentFile(resource("range.bam"), "rb") as s:
        hdr, recs = s.header, list(s.fetch(until_eof=True))
    with pysam.AlignmentFile(path, "wb", header=hdr) as o:
        for _ in range(repeat):
            for r in recs:
                o.write(r)
    return len(recs) * repeat


def test_unindexed_split_is_lossless(spark, tmp_path):
    big = str(tmp_path / "big.bam")          # no .bai -> byte-range split path
    total = _make_multiblock_bam(big)

    # baseline: force a single partition (huge split size)
    whole = (spark.read.format("bam")
             .option("useIndex", "false").option("bgzfSplitSize", str(10 ** 9))
             .load(big))
    baseline = multiset(whole.collect())
    assert whole.rdd.getNumPartitions() == 1
    assert sum(baseline.values()) == total

    # many small chunks -> many non-empty partitions; union must be identical
    split_df = (spark.read.format("bam")
                .option("useIndex", "false").option("bgzfSplitSize", "20000")
                .load(big))
    assert split_df.rdd.getNumPartitions() > 1
    split = multiset(split_df.collect())

    assert split == baseline          # lossless: same multiset of records, no gaps/dups
    assert sum(split.values()) == total
