"""lite-bfx-spark — pure-Python Spark 4 Data Source readers for genomics formats.

A JAR-free, ``pip``-installable equivalent of the Java DataSource V2 library. Parsing is
done with pysam (bundled htslib); schemas match the JAR field-for-field.

Usage::

    import litebfx
    litebfx.register_all(spark)
    df = spark.read.format("bam").load("/Volumes/cat/sch/vol/sample.bam")

See ``docs/proposals/python-data-source.md`` for the design and ``TASKS.md`` for status.
"""

__version__ = "0.0.0"

__all__ = ["register_all", "__version__"]

# Short names registered by register_all(). CRAM shares the BAM reader (isCram=True),
# matching the JAR where CramDataSource delegates to the BAM chain.
_FORMATS = ("bam", "cram", "vcf", "fasta", "fastq", "bed")


_PUSHDOWN_CONF = "spark.sql.python.filterPushdown.enabled"


def register_all(spark):
    """Register every genomics format on ``spark`` so ``spark.read.format(name)`` works.

    Imports are deferred so that ``import litebfx`` (and the pure-Python submodules) do not
    require pyspark until a session is actually available.

    BAM/CRAM, VCF, and BED implement ``pushFilters()`` for Spark 4.1+ index-guided reads.
    That conf defaults to false on every Spark 4.1+ build (Databricks included) — Spark
    treats an implemented-but-unwanted pushFilters() as a planning error rather than
    silently ignoring it, and on locked-down environments like Databricks Serverless a user
    may not be able to flip the conf themselves. A DataSourceReader can't see this conf: by
    the time pushFilters()/reader() run, they're in an isolated worker process with no
    SparkSession. register_all() is the one place that still holds a real session, so the
    check happens once here and picks a reader class that either has pushFilters() or
    doesn't — the class itself has to differ, since Spark's check is "is pushFilters
    overridden at all", not something a no-op body could satisfy.
    """
    from .bam import (BamDataSource, CramDataSource,
                      _BamDataSourcePushdown, _CramDataSourcePushdown)
    from .bed import BedDataSource, _BedDataSourcePushdown
    from .fasta import FastaDataSource
    from .fastq import FastqDataSource
    from .vcf import VcfDataSource, _VcfDataSourcePushdown

    pushdown = str(spark.conf.get(_PUSHDOWN_CONF, "false")).lower() == "true"

    for ds in (_BamDataSourcePushdown if pushdown else BamDataSource,
               _CramDataSourcePushdown if pushdown else CramDataSource,
               _VcfDataSourcePushdown if pushdown else VcfDataSource,
               FastaDataSource,
               FastqDataSource,
               _BedDataSourcePushdown if pushdown else BedDataSource):
        spark.dataSource.register(ds)
    return spark
