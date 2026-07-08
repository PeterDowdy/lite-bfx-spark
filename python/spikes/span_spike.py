"""
Stress the guesser's mid-record resync: build a BAM whose records are larger than a
64 KB BGZF block, so most block boundaries fall in the MIDDLE of a record (uoffset != 0).
The guesser must then skip the remainder of a straddling record to the next real start.
Same losslessness assertion as big_spike.
"""
import os, math, tempfile
from array import array
import pysam
import bam_spike as S

L = 100000          # per-read sequence length (>64KB block -> records span blocks)
NREADS = 40
NCHUNKS = 8

tmp = tempfile.mkdtemp()
path = os.path.join(tmp, "long.bam")
header = {"HD": {"VN": "1.6"}, "SQ": [{"SN": "chr1", "LN": 300000}]}
seq = ("ACGT" * (L // 4))[:L]
quals = array("B", [30] * L)

with pysam.AlignmentFile(path, "wb", header=header) as out:
    for i in range(NREADS):
        a = pysam.AlignedSegment()
        a.query_name = f"read{i:04d}"
        a.flag = 4                      # unmapped -> refID/pos = -1 (valid, no contig-length constraint)
        a.reference_id = -1
        a.reference_start = -1
        a.query_sequence = seq
        a.query_qualities = quals
        out.write(a)
assert not os.path.exists(path + ".bai")

raw = open(path, "rb").read()
blocks, U = S.walk_blocks(raw)
hend, n_ref = S.header_end(U)
print(f"[setup] long.bam: {len(raw)} bytes, {len(U)} decompressed, {len(blocks)} blocks, "
      f"{NREADS} reads of {L}bp (each spans ~{math.ceil(L*1.5/65536)} blocks)")

def key(r):
    return (r.query_name, r.reference_id, r.reference_start, r.flag)

with pysam.AlignmentFile(path, "rb") as af:
    full = [key(r) for r in af.fetch(until_eof=True)]

Sz = math.ceil(len(raw) / NCHUNKS)
resolve = lambda P: (S.find_next_block(raw, P) if P < len(raw) else None)
starts = sorted({resolve(i * Sz) for i in range(NCHUNKS)} - {None})
ustart_of = {c: us for c, us, ul in blocks}

collected, uoffsets = [], []
for p, coff in enumerate(starts):
    stop = starts[p + 1] if p + 1 < len(starts) else len(raw) + 1
    g_abs = S.guess_record_start(U, max(ustart_of[coff], hend), n_ref)
    v = S.abs_to_voffset(blocks, g_abs)
    uoffsets.append(v & 0xFFFF)
    part = []
    with pysam.AlignmentFile(path, "rb") as af:
        af.seek(v)
        while True:
            vt = af.tell()
            if (vt >> 16) >= stop:
                break
            try:
                r = next(af)
            except StopIteration:
                break
            part.append(key(r))
    collected.append(part)

flat = [x for part in collected for x in part]
mid_record_starts = sum(1 for u in uoffsets if u != 0)
print(f"[split] {len(starts)} partitions; block-start uoffsets={uoffsets} "
      f"({mid_record_starts} landed mid-record, needing resync)")
print(f"[verify] records: partitions={len(flat)} full={len(full)} "
      f"count_match={len(flat) == len(full)}")
print(f"[verify] concatenation == full sequential read: {flat == full}")
print(f"\nRESULT: mid-record-resync split "
      f"{'PASSES' if flat == full and mid_record_starts > 0 else 'INCONCLUSIVE/FAILS'}")
