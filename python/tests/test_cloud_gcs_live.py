"""Direct gs:// reads against a real GCS bucket -- pysam/htslib's native GCS backend, no FUSE
mount, no emulator.

Unlike test_cloud_gcs.py (fake-gcs-server, orchestration-layer only), this is the actual
htslib-native read path: htslib has zero endpoint-override capability for GCS (confirmed by
binary inspection), so a real bucket is the *only* way to exercise `pysam.AlignmentFile
("gs://...")` at all -- there is no MinIO-equivalent for GCS. Skipped entirely (not failed)
unless GOOGLE_APPLICATION_CREDENTIALS is set -- run via `docker compose run --rm
python-test-gcs-live` (or python-test-databricks-gcs-live), not in default CI.
"""

import pytest

pytestmark = pytest.mark.spark


def test_gcs_live_full_scan_matches_local(spark, gcs_bucket_live):
    import pysam
    from conftest import resource
    got = spark.read.format("bam").option("indexPath", f"{gcs_bucket_live}/range.bam.bai") \
        .load(f"{gcs_bucket_live}/range.bam").count()
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch(until_eof=True))
    assert got == exp > 0


def test_gcs_live_region_query_matches_local(spark, gcs_bucket_live):
    import pysam
    from conftest import resource
    got = (spark.read.format("bam")
           .option("indexPath", f"{gcs_bucket_live}/range.bam.bai")
           .option("region", "CHROMOSOME_I:1-2000")
           .load(f"{gcs_bucket_live}/range.bam").count())
    with pysam.AlignmentFile(resource("range.bam"), "rb") as af:
        exp = sum(1 for _ in af.fetch("CHROMOSOME_I", 0, 2000))
    assert got == exp > 0


def test_gcs_live_fasta_contig_matches_local(spark, gcs_bucket_live):
    import pysam
    from conftest import resource
    got = (spark.read.format("fasta")
           .option("indexPath", f"{gcs_bucket_live}/realn01.fa.fai")
           .load(f"{gcs_bucket_live}/realn01.fa").collect())
    with pysam.FastaFile(resource("realn01.fa")) as fa:
        exp_contigs = set(fa.references)
    assert {r.name for r in got} == exp_contigs
    assert len(got) > 0
