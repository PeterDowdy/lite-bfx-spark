"""
Multi-block spike: simulate the REAL unindexed-BAM chunked split and prove it is lossless.

Build a BAM large enough to span many BGZF blocks, then partition it into N compressed-byte
chunks exactly as the proposal describes:
  chunk i boundary = i * S  ->  find_next_block()  ->  guess record start  ->  seek(voffset)
  ->  read until the next record's block offset reaches the next chunk boundary.
Assert the concatenation of all partitions == a full sequential read (order + content),
i.e. every record is read exactly once (no gaps, no duplicates).
"""
import os, sys, math, tempfile
import pysam
import bam_spike as S

SRC = os.path.abspath(sys.argv[1])
REPEAT = int(sys.argv[2]) if len(sys.argv) > 2 else 40
NCHUNKS = int(sys.argv[3]) if len(sys.argv) > 3 else 8

tmp = tempfile.mkdtemp()
big = os.path.join(tmp, "big.bam")

# ---- build a multi-block BAM (no index) by repeating the source records ----
with pysam.AlignmentFile(SRC, "rb") as src:
    hdr = src.header
    recs = list(src.fetch(until_eof=True))
with pysam.AlignmentFile(big, "wb", header=hdr) as out:
    for _ in range(REPEAT):
        for r in recs:
            out.write(r)
assert not os.path.exists(big + ".bai")

raw = open(big, "rb").read()
blocks, U = S.walk_blocks(raw)
hend, n_ref = S.header_end(U)
data_blocks = [c for c, us, ul in blocks if ul > 0]
print(f"[setup] big.bam: {len(raw)} bytes, {len(U)} decompressed, "
      f"{len(blocks)} BGZF blocks ({len(data_blocks)} with data), "
      f"{len(recs)*REPEAT} records")

# ---- ground truth: full sequential read ----
def key(r):
    return (r.query_name, r.reference_id, r.reference_start, r.flag, r.cigarstring)

with pysam.AlignmentFile(big, "rb") as af:
    full = [key(r) for r in af.fetch(until_eof=True)]

# ---- resolve chunk boundaries to real block offsets ----
Sz = math.ceil(len(raw) / NCHUNKS)
def resolve(P):
    if P >= len(raw):
        return None
    try:
        return S.find_next_block(raw, P)
    except ValueError:
        return None

# unique, ordered block starts that chunk boundaries map to (dedup empty/again-same)
starts = sorted({c for c in (resolve(i * Sz) for i in range(NCHUNKS)) if c is not None})
ustart_of = {c: us for c, us, ul in blocks}
print(f"[split] S={Sz} bytes/chunk; {NCHUNKS} boundaries -> "
      f"{len(starts)} distinct block starts: {starts}")

# ---- read each partition [starts[p], starts[p+1]) ----
collected = []
for p, coff in enumerate(starts):
    stop = starts[p + 1] if p + 1 < len(starts) else len(raw) + 1   # last -> EOF
    g_from = max(ustart_of[coff], hend)                # skip header for the first block
    g_abs = S.guess_record_start(U, g_from, n_ref)
    v = S.abs_to_voffset(blocks, g_abs)
    part = []
    with pysam.AlignmentFile(big, "rb") as af:
        af.seek(v)
        while True:
            vt = af.tell()
            if (vt >> 16) >= stop:                     # next record belongs to next partition
                break
            try:
                r = next(af)
            except StopIteration:
                break
            part.append(key(r))
    collected.append(part)
    print(f"    part {p}: block_coffset={coff:>6} guessed_uoffset={v & 0xffff:>5} "
          f"stop_coffset={stop:<7} records={len(part)}")

flat = [x for part in collected for x in part]
lossless = flat == full
counts_ok = len(flat) == len(full)
# also verify no partition overlap by checking sequential concatenation equals order
print(f"\n[verify] total records: partitions={len(flat)} full={len(full)}  "
      f"count_match={counts_ok}")
print(f"[verify] concatenation == full sequential read (order+content): {lossless}")
print(f"\nRESULT: multi-block unindexed split {'PASSES' if lossless and counts_ok else 'FAILS'}")
