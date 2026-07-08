# Spikes

Throwaway validation scripts that de-risked the [Python Data Source
proposal](../../docs/proposals/python-data-source.md). They are **not** part of the
`litebfx` package — they are kept for reproducibility of the design decisions.

Run them against the checked-in fixtures with the dev venv (pysam required):

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install pysam pyspark
python spikes/bam_spike.py     ../core/src/test/resources/range.bam
python spikes/big_spike.py     ../core/src/test/resources/range.bam 40 8
python spikes/guesser_spike.py ../core/src/test/resources/range.bam 40
python spikes/cram_probe.py    ../core/src/test/resources/range.cram
```

| Script | Question it answers | Result |
|---|---|---|
| `bam_spike.py` | Does `pysam.AlignmentFile.seek()` accept an **externally computed** virtual offset on an **index-less** open? | **Yes** — Part A passes |
| `guesser_spike.py` | Does the pure-Python BGZF block finder + BAM record-boundary guesser resync correctly from an arbitrary offset? | **Yes** — 250k offsets (249k mid-record), 0 false positives |
| `big_spike.py` | Is an N-way compressed-byte chunked split of a multi-block BAM lossless? | **Yes** — 4480 records, concat == full sequential read |
| `span_spike.py` | (helper) build a BAM whose records span BGZF blocks | supporting fixture generator |
| `cram_probe.py` | Can we enumerate CRAM containers in pure Python, and does `pysam.index()` build a CRAI without a reference? | **Yes** to both — container walk sums to 112 records; CRAI built (93 B) |

`bam_spike.py` exposes reusable helpers (`walk_blocks`, `find_next_block`,
`valid_record`, `guess_record_start`); the production versions of these live in
`src/litebfx/bgzf.py`.
