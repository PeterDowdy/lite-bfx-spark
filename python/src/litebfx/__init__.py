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


def register_all(spark):
    """Register every genomics format on ``spark`` so ``spark.read.format(name)`` works.

    Imports are deferred so that ``import litebfx`` (and the pure-Python submodules) do not
    require pyspark until a session is actually available.
    """
    from .bam import BamDataSource, CramDataSource
    from .bed import BedDataSource
    from .fasta import FastaDataSource
    from .fastq import FastqDataSource
    from .vcf import VcfDataSource

    for ds in (BamDataSource, CramDataSource, VcfDataSource,
               FastaDataSource, FastqDataSource, BedDataSource):
        spark.dataSource.register(ds)
    return spark
