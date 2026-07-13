"""Direct s3:// reads against MinIO -- pysam/htslib's native S3 backend, no FUSE mount.

Correctness only (full scan vs. region-query counts, cross-checked against a local pysam
read of the same fixture) -- no byte-transfer-reduction assertion, since Python has no
Hadoop-FileSystem-statistics equivalent to measure it against; see the design plan for why
that's an accepted v1 gap. Skipped entirely (not failed) unless S3_ENDPOINT is set -- run via
`docker compose run --rm python-test-s3`, not in default CI.
"""

import pytest

pytestmark = pytest.mark.spark


def test_s3_full_scan_matches_local(spark, s3_bucket):
    import pysam
    from conftest import resource
    got = spark.read.format("bam").option("indexPath", f"{s3_bucket}/range.bam.bai") \
        .load(f"{s3_bucket}/range.bam").count()
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch(until_eof=True))
    assert got == exp > 0


def test_s3_region_query_matches_local(spark, s3_bucket):
    import pysam
    from conftest import resource
    got = (spark.read.format("bam")
           .option("indexPath", f"{s3_bucket}/range.bam.bai")
           .option("region", "CHROMOSOME_I:1-2000")
           .load(f"{s3_bucket}/range.bam").count())
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch("CHROMOSOME_I", 0, 2000))
    assert got == exp > 0


def test_s3_fasta_contig_matches_local(spark, s3_bucket):
    import pysam
    from conftest import resource
    got = (spark.read.format("fasta")
           .option("indexPath", f"{s3_bucket}/realn01.fa.fai")
           .load(f"{s3_bucket}/realn01.fa").collect())
    with pysam.FastaFile(resource("realn01.fa")) as fa:
        exp_contigs = set(fa.references)
    assert {r.name for r in got} == exp_contigs
    assert len(got) > 0
