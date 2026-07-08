"""Schema parity — Python schemas must match the Java ``*Schema`` classes exactly.

Expected (name, type, nullable) tuples are transcribed independently from the Java source,
so this is a true parity check rather than a self-comparison. Needs only pyspark types
(no SparkSession).
"""

from litebfx import schemas


def _triples(struct):
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in struct.fields]


BAM_DESCRIPTIVE = [
    ("readName", "string", True), ("flags", "int", False),
    ("referenceName", "string", True), ("start", "bigint", True),
    ("mappingQuality", "int", True), ("cigar", "string", True),
    ("mateReferenceName", "string", True), ("mateStart", "bigint", True),
    ("insertSize", "int", True), ("sequence", "string", True),
    ("baseQualities", "string", True), ("attributes", "map<string,string>", True),
    ("start0", "bigint", True),
]
BAM_SAM = [
    ("qname", "string", True), ("flag", "int", False), ("rname", "string", True),
    ("pos", "bigint", True), ("mapq", "int", True), ("cigar", "string", True),
    ("rnext", "string", True), ("pnext", "bigint", True), ("tlen", "int", True),
    ("seq", "string", True), ("qual", "string", True),
    ("attributes", "map<string,string>", True), ("start0", "bigint", True),
]
VCF = [
    ("chrom", "string", False), ("pos", "int", False), ("id", "string", True),
    ("ref", "string", False), ("alt", "array<string>", True), ("qual", "double", True),
    ("filter", "string", True), ("info", "map<string,string>", False),
    ("format", "string", True), ("genotypes", "map<string,string>", True),
]
BED = [
    ("chrom", "string", False), ("chromStart", "bigint", False),
    ("chromEnd", "bigint", False), ("name", "string", True), ("score", "int", True),
    ("strand", "string", True), ("thickStart", "bigint", True),
    ("thickEnd", "bigint", True), ("itemRgb", "string", True),
    ("blockCount", "int", True), ("blockSizes", "string", True),
    ("blockStarts", "string", True),
]
FASTA = [("name", "string", False), ("sequence", "string", False), ("length", "bigint", False)]
FASTQ = [
    ("readName", "string", False), ("sequence", "string", False),
    ("baseQualities", "string", False), ("description", "string", True),
    ("readNumber", "int", True),
]


def test_bam_schema():
    assert _triples(schemas.bam_schema()) == BAM_DESCRIPTIVE
    assert _triples(schemas.bam_schema(sam_names=True)) == BAM_SAM


def test_vcf_schema():
    assert _triples(schemas.VCF_SCHEMA) == VCF


def test_bed_schema():
    assert _triples(schemas.BED_SCHEMA) == BED


def test_fasta_schema():
    assert _triples(schemas.FASTA_SCHEMA) == FASTA


def test_fastq_schema():
    assert _triples(schemas.FASTQ_SCHEMA) == FASTQ
