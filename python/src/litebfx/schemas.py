"""Spark schemas — field-for-field mirrors of the Java ``*Schema`` classes.

Column names, types, nullability, and coordinate conventions match the JAR so a DataFrame
from this reader is indistinguishable from the JAR's. Parity is asserted in the tests
against golden schema JSON.
"""

from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, MapType, DoubleType, StringType,
    StructField, StructType,
)

_STR, _INT, _LONG, _DBL = StringType(), IntegerType(), LongType(), DoubleType()
_STRMAP = MapType(_STR, _STR, True)

# --- BAM / SAM / CRAM -------------------------------------------------------------------
COLUMN_NAMES_OPTION = "columnnames"   # option keys arrive lowercased
_SAM_NAMES = "sam"


def bam_schema(sam_names: bool = False) -> StructType:
    """13-column BAM/SAM/CRAM schema. ``sam_names`` selects the canonical SAM field names."""
    def n(descriptive, sam):
        return sam if sam_names else descriptive
    return StructType([
        StructField(n("readName", "qname"),          _STR,  True),
        StructField(n("flags", "flag"),              _INT,  False),
        StructField(n("referenceName", "rname"),     _STR,  True),
        StructField(n("start", "pos"),               _LONG, True),   # 1-based (SAM spec)
        StructField(n("mappingQuality", "mapq"),     _INT,  True),
        StructField("cigar",                         _STR,  True),
        StructField(n("mateReferenceName", "rnext"), _STR,  True),
        StructField(n("mateStart", "pnext"),         _LONG, True),   # 1-based (SAM spec)
        StructField(n("insertSize", "tlen"),         _INT,  True),
        StructField(n("sequence", "seq"),            _STR,  True),
        StructField(n("baseQualities", "qual"),      _STR,  True),
        StructField("attributes",                    _STRMAP, True),  # SAM TYPE:VALUE form
        StructField("start0",                        _LONG, True),   # 0-based (BED-compatible)
    ])


def is_sam_column_names(options) -> bool:
    for k, v in (options or {}).items():
        if k.lower() == COLUMN_NAMES_OPTION:
            return str(v).lower() == _SAM_NAMES
    return False


# --- VCF / BCF --------------------------------------------------------------------------
VCF_SCHEMA = StructType([
    StructField("chrom",     _STR,  False),
    StructField("pos",       _INT,  False),   # 1-based
    StructField("id",        _STR,  True),
    StructField("ref",       _STR,  False),
    StructField("alt",       ArrayType(_STR, True), True),
    StructField("qual",      _DBL,  True),
    StructField("filter",    _STR,  True),
    StructField("info",      _STRMAP, False),
    StructField("format",    _STR,  True),
    StructField("genotypes", _STRMAP, True),
])

# --- BED --------------------------------------------------------------------------------
BED_SCHEMA = StructType([
    StructField("chrom",       _STR,  False),
    StructField("chromStart",  _LONG, False),  # 0-based (BED spec)
    StructField("chromEnd",    _LONG, False),  # 0-based exclusive
    StructField("name",        _STR,  True),
    StructField("score",       _INT,  True),
    StructField("strand",      _STR,  True),
    StructField("thickStart",  _LONG, True),
    StructField("thickEnd",    _LONG, True),
    StructField("itemRgb",     _STR,  True),
    StructField("blockCount",  _INT,  True),
    StructField("blockSizes",  _STR,  True),
    StructField("blockStarts", _STR,  True),
])

# --- FASTA ------------------------------------------------------------------------------
FASTA_SCHEMA = StructType([
    StructField("name",     _STR,  False),
    StructField("sequence", _STR,  False),
    StructField("length",   _LONG, False),
])

# --- FASTQ ------------------------------------------------------------------------------
FASTQ_SCHEMA = StructType([
    StructField("readName",      _STR, False),
    StructField("sequence",      _STR, False),
    StructField("baseQualities", _STR, False),
    StructField("description",   _STR, True),
    StructField("readNumber",    _INT, True),   # 1=R1, 2=R2 from filename; null if unknown
])
