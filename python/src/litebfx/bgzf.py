"""BGZF block finder + BAM record-boundary guesser for unindexed BAM splitting.

Productionized from ``spikes/bam_spike.py`` (validated: 250k offsets, 0 false positives;
8-way split lossless). Given an arbitrary compressed byte offset, locate the next BGZF
block and the first BAM record start at/after it, and return a virtual file offset
``(coffset << 16) | uoffset`` that ``pysam.AlignmentFile.seek()`` accepts.

Reads only a bounded window from the file, so it works on FUSE-mounted paths without
pulling the whole object.
"""

import struct
import zlib

_MAGIC = b"\x1f\x8b\x08\x04"          # gzip ID1/ID2, CM=deflate, FLG=FEXTRA
MAX_BLOCK = 1 << 16                    # 64 KiB, the BGZF maximum block size
_DEFAULT_WINDOW = 1 << 20             # 1 MiB: comfortably spans a block + record chain

_u16 = lambda b, o: struct.unpack_from("<H", b, o)[0]
_i32 = lambda b, o: struct.unpack_from("<i", b, o)[0]
_u32 = lambda b, o: struct.unpack_from("<I", b, o)[0]


def _block_len(buf, off):
    """Total BGZF block length at ``off`` from the BC subfield, or None if not a header."""
    if buf[off:off + 4] != _MAGIC or off + 12 > len(buf):
        return None
    xlen = _u16(buf, off + 10)
    i, end = off + 12, off + 12 + xlen
    if end > len(buf):
        return None
    while i + 4 <= end:
        si1, si2, slen = buf[i], buf[i + 1], _u16(buf, i + 2)
        if si1 == 0x42 and si2 == 0x43:      # 'BC'
            return _u16(buf, i + 4) + 1
        i += 4 + slen
    return None


def find_block(buf, start):
    """Offset in ``buf`` of the next validated BGZF block header at/after ``start``, or None."""
    i, limit = start, len(buf) - 4
    while i <= limit:
        if buf[i:i + 4] == _MAGIC:
            bl = _block_len(buf, i)
            if bl is not None:
                end = i + bl
                # Chain-validate against a stray magic in compressed data: the block must
                # abut EOF, the next header, or run past our window (can't disprove).
                if end >= len(buf) or buf[end:end + 4] == _MAGIC:
                    return i
        i += 1
    return None


def valid_record(U, u, n_ref, depth=3):
    """True if a self-consistent BAM record starts at ``U[u]`` and chains ``depth`` more."""
    if u + 36 > len(U):
        return False
    bs = _i32(U, u)
    end = u + 4 + bs
    if bs < 32 or end > len(U):
        return False
    ref_id, pos = _i32(U, u + 4), _i32(U, u + 8)
    l_read_name = U[u + 12]
    n_cigar = _u16(U, u + 16)
    l_seq = _i32(U, u + 20)
    next_ref, next_pos = _i32(U, u + 24), _i32(U, u + 28)
    if not (-1 <= ref_id < n_ref) or not (-1 <= next_ref < n_ref):
        return False
    if pos < -1 or next_pos < -1 or l_read_name < 1 or l_seq < 0:
        return False
    name_nul = u + 36 + l_read_name - 1          # read_name is NUL-terminated
    if name_nul >= len(U) or U[name_nul] != 0:
        return False
    if bs < 32 + l_read_name + 4 * n_cigar + (l_seq + 1) // 2 + l_seq:
        return False
    if depth and end < len(U):
        return valid_record(U, end, n_ref, depth - 1)
    return True


def _guess(U, start, n_ref):
    u = start
    while u < len(U):
        if valid_record(U, u, n_ref):
            return u
        u += 1
    return None


def split_start_voffset(path, start_byte, n_ref, window=_DEFAULT_WINDOW):
    """Virtual offset of the first BAM record whose block begins at/after ``start_byte``.

    Returns None when the split contains no record start (e.g. it falls past the last data
    block), in which case the partition is legitimately empty.
    """
    with open(path, "rb") as f:
        f.seek(start_byte)
        buf = f.read(window)

    bo = find_block(buf, 0)
    if bo is None:
        return None

    # Decompress the window's blocks into one buffer, tracking each block's absolute
    # compressed offset so a found uncompressed position maps back to a virtual offset.
    segs, U, off = [], bytearray(), bo
    while off < len(buf):
        bl = _block_len(buf, off)
        if bl is None or off + bl > len(buf):
            break
        xlen = _u16(buf, off + 10)
        cdata = buf[off + 12 + xlen: off + bl - 8]
        isize = _u32(buf, off + bl - 4)
        try:
            udata = zlib.decompress(cdata, -15) if cdata else b""
        except zlib.error:
            break
        if len(udata) != isize:
            break
        segs.append((len(U), start_byte + off, len(udata)))
        U += udata
        off += bl

    u = _guess(U, 0, n_ref)
    if u is None:
        return None
    for u_start, coff_abs, _ulen in reversed(segs):
        if u >= u_start:
            return (coff_abs << 16) | (u - u_start)
    return None
