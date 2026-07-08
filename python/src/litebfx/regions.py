"""Region parsing for pushed reads.

Two ways a caller narrows a read to a genomic interval:

* the explicit ``.option("region", "chr1:1000-2000")`` option — version-independent, the
  reliable path on Spark 4.0 which has no filter-pushdown hook for Python data sources;
* ``DataSourceReader.pushFilters()`` on Spark 4.1+, parsed here defensively by duck-typing
  the filter objects (equality on the reference-name column, range on the coordinate
  column) so we do not hard-depend on a specific 4.1 filter API.

A :class:`Region` uses 1-based inclusive coordinates (SAM/VCF convention). ``pysam.fetch``
takes 0-based half-open, so callers convert at the boundary.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Region:
    contig: str
    start: Optional[int] = None   # 1-based inclusive; None => whole contig
    end: Optional[int] = None     # 1-based inclusive

    def fetch_args(self):
        """(contig, start0, end) for ``pysam.*.fetch`` — 0-based half-open, Nones dropped."""
        start0 = None if self.start is None else max(self.start - 1, 0)
        return self.contig, start0, self.end


def parse_region(spec: str) -> Optional[Region]:
    """Parse ``chr1``, ``chr1:1000``, or ``chr1:1000-2000`` (commas allowed). None if empty."""
    if not spec:
        return None
    spec = spec.strip()
    if ":" not in spec:
        return Region(spec)
    contig, _, rng = spec.rpartition(":")
    rng = rng.replace(",", "")
    if "-" in rng:
        lo, _, hi = rng.partition("-")
        return Region(contig, int(lo), int(hi))
    pos = int(rng)
    return Region(contig, pos, pos)


# --- Spark 4.1+ pushFilters, duck-typed -------------------------------------------------

def _attr(f):
    """Column name a filter targets, or None. Handles a few known shapes."""
    for name in ("attribute", "column", "columnPath", "columnName"):
        v = getattr(f, name, None)
        if isinstance(v, str):
            return v
        if isinstance(v, (list, tuple)) and v:
            return v[-1]
    return None


def _cls(f):
    return type(f).__name__


def push_region(filters, ref_col, coord_col):
    """Interpret pushed filters into (Region-or-None, unhandled_filters).

    Equality on ``ref_col`` is fully handled (index guarantees exact match); range
    comparisons on ``coord_col`` are absorbed to narrow the index query but returned as
    *unhandled* so Spark re-checks them post-scan (index queries are overlap queries).
    Unknown filters are returned unhandled.
    """
    contig = start = end = None
    unhandled = []
    for f in filters:
        cls, col = _cls(f), _attr(f)
        val = getattr(f, "value", None)
        if cls in ("EqualTo",) and col == ref_col:
            contig = val
            continue
        if col == coord_col and cls in ("GreaterThan", "GreaterThanOrEqual",
                                        "LessThan", "LessThanOrEqual"):
            try:
                v = int(val)
            except (TypeError, ValueError):
                unhandled.append(f)
                continue
            if cls == "GreaterThan":
                start = v + 1 if start is None else max(start, v + 1)
            elif cls == "GreaterThanOrEqual":
                start = v if start is None else max(start, v)
            elif cls == "LessThan":
                end = v - 1 if end is None else min(end, v - 1)
            else:  # LessThanOrEqual
                end = v if end is None else min(end, v)
            unhandled.append(f)   # keep for Spark's post-scan correctness filter
            continue
        unhandled.append(f)
    region = Region(contig, start, end) if contig is not None else None
    return region, unhandled
