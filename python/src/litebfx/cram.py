"""CRAM DataSource.

CRAM shares the alignment reader with BAM (``is_cram=True``); this module re-exports the
entry point so ``litebfx.cram.CramDataSource`` resolves alongside the other formats.
"""

from .bam import CramDataSource

__all__ = ["CramDataSource"]
