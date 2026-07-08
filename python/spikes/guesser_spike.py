"""
Exhaustive guesser-correctness / resync test.

From EVERY byte offset in a real multi-record BAM's decompressed stream (the vast majority
of which are mid-record), assert the record-boundary guesser returns exactly the next true
record start — no false positives (landing before the real boundary) and no skips (landing
after it). This is the resync primitive the unindexed split depends on.
"""
import os, sys, bisect, tempfile
import pysam
import bam_spike as S

SRC = os.path.abspath(sys.argv[1])
REPEAT = int(sys.argv[2]) if len(sys.argv) > 2 else 40

tmp = tempfile.mkdtemp()
big = os.path.join(tmp, "big.bam")
with pysam.AlignmentFile(SRC, "rb") as src:
    hdr, recs = src.header, list(src.fetch(until_eof=True))
with pysam.AlignmentFile(big, "wb", header=hdr) as out:
    for _ in range(REPEAT):
        for r in recs:
            out.write(r)

raw = open(big, "rb").read()
_, U = S.walk_blocks(raw)
hend, n_ref = S.header_end(U)
truth = S.true_record_starts(U, hend)
truth_set = set(truth)

def next_true_start(a):
    i = bisect.bisect_left(truth, a)
    return truth[i] if i < len(truth) else None

# exhaustive over a wide window: every byte offset, almost all land mid-record
lo, hi = hend, min(len(U) - 40, hend + 250_000)
tested = mid = wrong = 0
first_fail = None
for a in range(lo, hi):
    expected = next_true_start(a)
    if expected is None:
        break
    got = S.guess_record_start(U, a, n_ref)
    tested += 1
    if a not in truth_set:
        mid += 1
    if got != expected:
        wrong += 1
        if first_fail is None:
            first_fail = (a, got, expected)

print(f"[data] big.bam records={len(truth)}, tested {tested} start offsets "
      f"over U[{lo}:{hi}] ({mid} of them mid-record)")
print(f"[result] guesser wrong={wrong}  first_failure={first_fail}")
print(f"\nRESULT: exhaustive guesser resync {'PASSES' if wrong == 0 else 'FAILS'}")
