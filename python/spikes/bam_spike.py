"""
Spike: unindexed BAM splitting in pure Python + pysam.

Validates the required Phase-2 path from the proposal:
  1. A pure-Python BGZF block finder locates block boundaries from an ARBITRARY byte offset.
  2. A pure-Python BAM record-boundary guesser finds a real record start in that block.
  3. pysam.AlignmentFile.seek(voffset) accepts that EXTERNALLY computed virtual offset
     on an INDEX-LESS open and reads the correct records from there.

Ground truth (true record boundaries) is parsed independently from the decompressed
stream, so the guesser is checked against reality, not against itself.
"""
import os, sys, shutil, struct, tempfile, zlib
import pysam

i32 = lambda b, o: struct.unpack_from("<i", b, o)[0]
u16 = lambda b, o: struct.unpack_from("<H", b, o)[0]
u32 = lambda b, o: struct.unpack_from("<I", b, o)[0]

# ---------------------------------------------------------------- BGZF layer
def block_len(raw, pos):
    xlen = u16(raw, pos + 10)
    extra = raw[pos + 12: pos + 12 + xlen]
    i = 0
    while i < len(extra):
        si1, si2, slen = extra[i], extra[i + 1], u16(extra, i + 2)
        if si1 == 0x42 and si2 == 0x43:      # 'BC' subfield -> BSIZE
            return u16(extra, i + 4) + 1
        i += 4 + slen
    raise ValueError("no BC subfield")

def is_block_header(raw, pos):
    if raw[pos:pos + 4] != b"\x1f\x8b\x08\x04":
        return False
    try:
        bl = block_len(raw, pos)
    except Exception:
        return False
    end = pos + bl
    return end == len(raw) or raw[end:end + 4] == b"\x1f\x8b\x08\x04"

def find_next_block(raw, start):
    i = start
    while i < len(raw) - 3:
        if is_block_header(raw, i):
            return i
        i += 1
    raise ValueError("no block header found")

def walk_blocks(raw):
    blocks, U, pos = [], bytearray(), 0
    while pos < len(raw):
        bl = block_len(raw, pos)
        xlen = u16(raw, pos + 10)
        cdata = raw[pos + 12 + xlen: pos + bl - 8]
        isize = u32(raw, pos + bl - 4)
        udata = zlib.decompress(cdata, -15) if cdata else b""
        assert len(udata) == isize
        blocks.append((pos, len(U), len(udata)))
        U += udata
        pos += bl
    return blocks, bytes(U)

def abs_to_voffset(blocks, abspos):
    for coff, ustart, ulen in blocks:
        if ustart <= abspos < ustart + ulen:
            return (coff << 16) | (abspos - ustart)
    raise KeyError(abspos)

# ---------------------------------------------------------- BAM record layer
def header_end(U):
    assert U[:4] == b"BAM\x01"
    p = 8 + i32(U, 4)
    n_ref = i32(U, p); p += 4
    for _ in range(n_ref):
        p += 4 + i32(U, p) + 4
    return p, n_ref

def true_record_starts(U, start):
    p, out = start, []
    while p + 4 <= len(U):
        bs = i32(U, p)
        if bs < 32 or p + 4 + bs > len(U):
            break
        out.append(p)
        p += 4 + bs
    return out

def valid_record(U, u, n_ref, depth=3):
    if u + 36 > len(U):
        return False
    bs = i32(U, u)
    end = u + 4 + bs
    if bs < 32 or end > len(U):
        return False
    refID, pos = i32(U, u + 4), i32(U, u + 8)
    l_read_name = U[u + 12]
    n_cigar = u16(U, u + 16)
    l_seq = i32(U, u + 20)
    next_refID, next_pos = i32(U, u + 24), i32(U, u + 28)
    if not (-1 <= refID < n_ref) or not (-1 <= next_refID < n_ref):
        return False
    if pos < -1 or next_pos < -1 or l_read_name < 1 or l_seq < 0:
        return False
    name_nul = u + 36 + l_read_name - 1
    if name_nul >= len(U) or U[name_nul] != 0:
        return False
    if bs < 32 + l_read_name + 4 * n_cigar + (l_seq + 1) // 2 + l_seq:
        return False
    if depth and end < len(U):
        return valid_record(U, end, n_ref, depth - 1)
    return True

def guess_record_start(U, from_abs, n_ref):
    u = from_abs
    while u < len(U):
        if valid_record(U, u, n_ref):
            return u
        u += 1
    raise ValueError("no record start")

# --------------------------------------------------------------- run spike
def main(bam):
    raw = open(bam, "rb").read()
    blocks, U = walk_blocks(raw)

    tmp = tempfile.mkdtemp()
    noidx = os.path.join(tmp, "noindex.bam")
    shutil.copy(bam, noidx)
    assert not os.path.exists(noidx + ".bai"), "should be index-less"

    hend, n_ref = header_end(U)
    truth = true_record_starts(U, hend)
    truth_set = set(truth)

    with pysam.AlignmentFile(noidx, "rb") as af:
        pysam_names = [r.query_name for r in af.fetch(until_eof=True)]
    print(f"[setup] index-less open OK; n_ref={n_ref}; records: "
          f"my_parser={len(truth)} pysam={len(pysam_names)} "
          f"-> {'MATCH' if len(truth) == len(pysam_names) else 'MISMATCH'}")

    with pysam.AlignmentFile(noidx, "rb") as af:
        mid = truth[len(truth) // 2]
        v = abs_to_voffset(blocks, mid)
        af.seek(v)
        rec = next(af)
        ln = U[mid + 12]
        exp = U[mid + 36: mid + 36 + ln - 1].decode()
        okA = rec.query_name == exp
        print(f"[A] seek(tell-derived voffset={v}) -> next()={rec.query_name!r} "
              f"expected={exp!r} -> {'PASS' if okA else 'FAIL'}")

    print("[B] externally-computed offsets (raw byte scan -> block -> guesser -> seek):")
    allB = True
    for frac in (0.10, 0.25, 0.50, 0.75, 0.90):
        P = int(len(raw) * frac)
        coff = find_next_block(raw, P)
        base = next(us for c, us, ul in blocks if c == coff)
        g_abs = guess_record_start(U, base, n_ref)
        v = abs_to_voffset(blocks, g_abs)
        real = g_abs in truth_set
        tail = [s for s in truth if s >= g_abs]
        exp = [U[s + 36: s + 36 + U[s + 12] - 1].decode() for s in tail]
        with pysam.AlignmentFile(noidx, "rb") as af:
            af.seek(v)
            got = [r.query_name for r in af]
        match = got == exp
        allB &= real and match
        print(f"    split@{frac:>4}: byteP={P:>6} coffset={coff:>6} "
              f"uoffset={v & 0xffff:>5} voffset={v:>12} real_boundary={real} "
              f"records_from_here={len(got)} tail_matches={match} "
              f"-> {'PASS' if real and match else 'FAIL'}")

    print(f"\nRESULT: A={'PASS' if okA else 'FAIL'}  B={'PASS' if allB else 'FAIL'}  "
          f"=> unindexed-BAM-split spike {'PASSES' if okA and allB else 'FAILS'}")

if __name__ == "__main__":
    main(os.path.abspath(sys.argv[1]))
