"""
Evaluate unindexed CRAM splitting feasibility.

Two questions:
  (1) EXECUTOR side: does pysam expose any way to start reading a CRAM at a chosen
      container/byte offset without a CRAI (the analog of BAM's seek(voffset))?
  (2) DRIVER side: can we enumerate container byte offsets from the CRAM structure in
      pure Python (the analog of the JAR's driver-side container-header scan)?
"""
import os, sys, struct, shutil, tempfile
import pysam

CRAM = os.path.abspath(sys.argv[1])

# ---------------------------------------------------------------- (1) pysam surface
tmp = tempfile.mkdtemp()
noidx = os.path.join(tmp, "noindex.cram")
shutil.copy(CRAM, noidx)

recs = None
try:
    with pysam.AlignmentFile(noidx, "rc", check_sq=False) as af:
        recs = [r.query_name for r in af.fetch(until_eof=True)]
    print(f"[pysam] index-less CRAM decode OK; records={len(recs)}")
except Exception as e:
    print(f"[pysam] index-less CRAM decode needs reference (expected): {type(e).__name__}: {e}")

# .tell()/.seek() surface on CRAM (does not require decoding records)
try:
    with pysam.AlignmentFile(noidx, "rc", check_sq=False) as af:
        t = af.tell()
        print(f"[pysam] CRAM .tell() at start = {t}  (hex {t:#x}); "
              f"low16={t & 0xffff} high={t >> 16}")
except Exception as e:
    print(f"[pysam] CRAM .tell() raised: {type(e).__name__}: {e}")

with pysam.AlignmentFile(noidx, "rc", check_sq=False) as af:
    api = [m for m in dir(af) if any(k in m.lower() for k in ("container", "slice", "seek", "tell", "offset"))]
print(f"[pysam] AlignmentFile seek/tell/container attrs: {api}")

# can we cheaply build a CRAI on the driver instead? (also reference-independent)
try:
    pysam.index(noidx)
    crai = noidx + ".crai"
    print(f"[pysam] pysam.index() built CRAI: exists={os.path.exists(crai)} "
          f"size={os.path.getsize(crai) if os.path.exists(crai) else 0}")
except Exception as e:
    print(f"[pysam] pysam.index() raised: {type(e).__name__}: {e}")

# ---------------------------------------------------------------- (2) pure-Python container scan
raw = open(CRAM, "rb").read()

def itf8(b, o):
    """Decode CRAM ITF8; return (value, next_offset)."""
    v = b[o]
    if v < 0x80:                       # 0xxxxxxx
        return v, o + 1
    if v < 0xC0:                       # 10xxxxxx +1
        return ((v & 0x3F) << 8) | b[o + 1], o + 2
    if v < 0xE0:                       # 110xxxxx +2
        return ((v & 0x1F) << 16) | (b[o + 1] << 8) | b[o + 2], o + 3
    if v < 0xF0:                       # 1110xxxx +3
        return ((v & 0x0F) << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3], o + 4
    # 1111xxxx +4  (low 4 bits of first byte hold the top nibble)
    return ((v & 0x0F) << 28) | (b[o + 1] << 24) | (b[o + 2] << 16) | (b[o + 3] << 8) | b[o + 4], o + 5

def ltf8(b, o):
    """Decode CRAM LTF8 (up to 8 bytes); return (value, next_offset)."""
    v = b[o]
    if v < 0x80:
        return v, o + 1
    n = 0
    while v & (0x80 >> n):
        n += 1
    if n >= 8:                          # 0xFF -> next 8 bytes
        val = int.from_bytes(b[o + 1:o + 9], "big"); return val, o + 9
    mask = (0xFF >> (n + 1))
    val = v & mask
    for i in range(n):
        val = (val << 8) | b[o + 1 + i]
    return val, o + 1 + n

def scan_containers(raw):
    assert raw[:4] == b"CRAM", "not a CRAM file"
    major, minor = raw[4], raw[5]
    p = 6 + 20                          # skip version + 20-byte file_id
    containers = []
    while p < len(raw):
        c_start = p
        length = struct.unpack_from("<i", raw, p)[0]; p += 4   # bytes of blocks after header
        ref_seq_id, p = itf8(raw, p)
        start_pos, p = itf8(raw, p)
        span, p = itf8(raw, p)
        n_records, p = itf8(raw, p)
        rec_counter, p = ltf8(raw, p)   # LTF8 in CRAM v3
        n_bases, p = ltf8(raw, p)
        n_blocks, p = itf8(raw, p)
        n_landmarks, p = itf8(raw, p)
        for _ in range(n_landmarks):
            _, p = itf8(raw, p)
        if major >= 3:
            p += 4                       # container header CRC32
        header_end = p
        containers.append((c_start, ref_seq_id, n_records, length))
        p = header_end + length          # skip blocks -> next container
        if length == 0 and n_records == 0 and c_start > 30:
            break                        # EOF container
    return major, minor, containers

major, minor, conts = scan_containers(raw)
print(f"\n[scan] CRAM v{major}.{minor}; {len(conts)} containers enumerated in pure Python:")
for i, (off, ref, nrec, ln) in enumerate(conts):
    kind = "header" if i == 0 else ("EOF" if nrec == 0 and ln == 0 else "data")
    print(f"    container {i}: byte_offset={off:>6} refSeqId={ref:>3} n_records={nrec:>4} "
          f"blocks_len={ln:>6}  [{kind}]")
data_recs = sum(nrec for i, (off, ref, nrec, ln) in enumerate(conts))
ref_count = "n/a (no reference)" if recs is None else len(recs)
verdict = "MATCH" if recs is not None and data_recs == len(recs) else \
          ("(pysam decode unavailable)" if recs is None else "MISMATCH")
print(f"[scan] total records across containers = {data_recs}  (pysam counted {ref_count}) -> {verdict}")
