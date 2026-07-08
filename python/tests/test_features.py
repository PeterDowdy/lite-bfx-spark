"""Multi-file reads, indexPath, _metadata, FASTQ splitting, unindexed-CRAM synthesis."""

import glob
import gzip
import os
import shutil

import pytest

from conftest import multiset, resource

pytestmark = pytest.mark.spark


# --- multi-file: glob + directory -------------------------------------------------------
def test_glob_multifile_fastq(spark):
    pattern = resource("TESTX_H7YRLADXX_S1_L00*_R1_001.fastq.gz")
    files = glob.glob(pattern)
    assert len(files) == 2
    total = spark.read.format("fastq").load(pattern).count()
    per_file = sum(spark.read.format("fastq").load(f).count() for f in files)
    assert total == per_file > 0


def test_directory_multifile_bam(spark, tmp_path):
    src, bai = resource("range.bam"), resource("range.bam.bai")
    for name in ("a.bam", "b.bam"):
        shutil.copy(src, tmp_path / name)
        shutil.copy(bai, tmp_path / (name + ".bai"))
    one = spark.read.format("bam").load(src).count()
    both = spark.read.format("bam").load(str(tmp_path)).count()   # directory listing
    assert both == 2 * one > 0


# --- indexPath option -------------------------------------------------------------------
def test_indexpath_bam(spark, tmp_path):
    import pysam
    src = resource("range.bam")
    x = str(tmp_path / "x.bam")
    shutil.copy(src, x)                                            # deliberately no co-located .bai
    assert not os.path.exists(x + ".bai")
    got = (spark.read.format("bam")
           .option("indexPath", resource("range.bam.bai"))
           .option("region", "CHROMOSOME_I:1-2000").load(x).count())
    with pysam.AlignmentFile(src, "rb") as af:
        exp = sum(1 for _ in af.fetch("CHROMOSOME_I", 0, 2000))
    assert got == exp > 0


# --- _metadata opt-in column ------------------------------------------------------------
def test_metadata_column_bam(spark):
    bam = resource("range.bam")
    df = spark.read.format("bam").option("metadata", "true").load(bam)
    assert "_metadata" in df.schema.fieldNames()
    row = df.select("_metadata").first()._metadata
    assert row.file_name == "range.bam"
    assert row.file_size == os.path.getsize(bam)
    assert row.file_path.endswith("range.bam")
    assert row.index_path.endswith(".bai")


def test_metadata_absent_by_default(spark):
    df = spark.read.format("bed").load(resource("example.bed.gz"))
    assert "_metadata" not in df.schema.fieldNames()


# --- FASTQ uncompressed byte-range splitting (losslessness, not parity to the JAR) ------
def test_fastq_uncompressed_split_lossless(spark, tmp_path):
    import pysam
    plain = str(tmp_path / "reads_R1_001.fastq")   # keep R1 in the name -> readNumber == 1
    with gzip.open(resource("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz"), "rt") as src, \
            open(plain, "w") as dst:
        shutil.copyfileobj(src, dst)

    whole = spark.read.format("fastq").option("minSplitBytes", str(10 ** 9)).load(plain)
    split = spark.read.format("fastq").option("minSplitBytes", "100000").load(plain)
    assert whole.rdd.getNumPartitions() == 1
    assert split.rdd.getNumPartitions() > 1

    with pysam.FastxFile(plain) as fx:
        oracle = multiset([(e.name, e.sequence, e.quality, e.comment or None, 1) for e in fx])
    assert multiset(split.collect()) == oracle
    assert multiset(whole.collect()) == oracle


# --- unindexed CRAM: driver synthesizes a CRAI, then per-contig -------------------------
def _make_sorted_cram(d):
    import pysam
    ref = os.path.join(d, "ref.fa")
    with open(ref, "w") as f:
        f.write(">chr1\n" + "ACGT" * 250 + "\n")                  # 1000 bp
    pysam.faidx(ref)
    header = {"HD": {"VN": "1.6", "SO": "coordinate"}, "SQ": [{"SN": "chr1", "LN": 1000}]}
    cram = os.path.join(d, "s.cram")
    with pysam.AlignmentFile(cram, "wc", header=header, reference_filename=ref) as out:
        for i in range(10):
            a = pysam.AlignedSegment()
            a.query_name = f"r{i:02d}"
            a.flag = 0
            a.reference_id = 0
            a.reference_start = i * 50
            a.mapping_quality = 60
            a.cigarstring = "20M"
            a.query_sequence = "ACGTACGTACGTACGTACGT"
            a.query_qualities = pysam.qualitystring_to_array("I" * 20)
            out.write(a)
    if os.path.exists(cram + ".crai"):
        os.remove(cram + ".crai")
    return cram, ref


def test_unindexed_cram_synthesized(spark, tmp_path):
    import pysam
    cram, ref = _make_sorted_cram(str(tmp_path))
    assert not os.path.exists(cram + ".crai")

    got = sorted(r.readName for r in
                 spark.read.format("cram").option("referenceFile", ref).load(cram).collect())
    with pysam.AlignmentFile(cram, "rc", reference_filename=ref) as af:
        exp = sorted(r.query_name for r in af.fetch(until_eof=True))
    assert got == exp and len(got) == 10
    assert os.path.exists(cram + ".crai"), "reader should have synthesized a CRAI"
