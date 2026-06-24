# Partitioning and indexes

This document explains how lite-bfx-spark plans Spark partitions, uses genomics indexes for partition pruning, and interacts with Spark's filter and projection pushdown mechanisms.

---

## Overview

A Spark partition in this library is a `(filePath, byteRange, queryParams, hadoopConf)` tuple. Each executor opens the file at the given path, seeks to the byte range using the Hadoop `FileSystem`, and iterates records until the range is exhausted. When a genomics index is present it drives the plan — how many partitions there are and which byte ranges they cover. When no index is present, the file is still split for parallelism: by byte range (BAM, SAM, plain-text VCF/BED) or by a lightweight container-header scan (CRAM). An index-free split gives **read parallelism but no I/O reduction** — the whole file is read across the union of partitions.

The core insight is that genomics index formats (BAI, CRAI, tabix) map genomic coordinates to **virtual file offsets (VFOs)** within the BGZF-compressed file. By reading the index on the driver, the scan planner can translate a genomic query into a set of byte ranges, one per Spark partition, without reading any alignment data.

---

## BAM: VFO-based splitting with BAI

BAI (BAM index) files store, for each reference sequence, the set of BGZF block offsets that contain alignments on that reference. The htsjdk `BAMFileSpan` API exposes these as virtual file offset (VFO) chunks.

### When a BAI is present and no region filter is pushed (hybrid splitting)

The scan reads the BAM header and the BAI on the driver. Each reference becomes one partition — but a reference whose compressed data span exceeds `indexedSplitSize` (default 128 MiB) is **sub-split** into several partitions, so one huge reference (e.g. chr1 in a WGS BAM) no longer dominates. One partition for unmapped reads is appended.

Split boundaries are **record-start virtual file offsets** read straight from the BAI (chunk starts plus linear-index entries, harvested across the reference's coordinate range and grouped into bins of ~`indexedSplitSize` compressed bytes). Because every boundary is an exact record start, the executor seeks directly there with no block-boundary guessing, and consecutive splits of a reference are disjoint at record granularity — **no records dropped or duplicated**. A small per-partition reference filter drops any neighbouring-reference reads that share a boundary BGZF block.

```
WGS sample.bam, indexedSplitSize=128 MiB:
  chr1 (2.1 GiB of reads) → 17 VFO splits  [vfo0,vfo1) [vfo1,vfo2) … (each ~128 MiB)
  chr2 (2.0 GiB)          → 16 VFO splits
  …
  chrM (small)            → 1 partition
  unmapped                → samReader.queryUnmapped()
```

Total records across all partitions = all records in the file, each exactly once.

#### Many references (skew-aware fallback)

When a file has more references than `numPartitions`, one-partition-per-reference would blow the cap, so the scan groups references instead. The grouping is **skew-aware**: it still reads the BAI on the driver and measures each reference's compressed byte span, then

- any single reference whose span exceeds `indexedSplitSize` is **sub-split** into VFO partitions exactly as above (so one heavy reference among thousands still gets its own size-based splits), and
- the remaining references are **bin-packed by byte span** (greedy longest-processing-time) into the leftover partition budget — one `QueryInterval[]` per group.

References with no indexed reads contribute nothing and are dropped; one unmapped partition is appended. Because groups are balanced by bytes rather than by reference count, a hot reference can no longer be bundled with arbitrary neighbours into a single straggler task.

```
panel.bam, 5000 references, numPartitions=200, indexedSplitSize=128 MiB:
  chrBig (900 MiB of reads)        → 7 VFO splits          (exceeds split size)
  4999 small refs (≤128 MiB each)  → bin-packed into 193 byte-balanced groups
  unmapped                         → samReader.queryUnmapped()
  ≈ 7 + 193 + 1 = 201 partitions, each ~equal compressed bytes
```


### When a region filter is pushed

When the planner receives a filter like `referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370`, the scan takes the BAI span overlapping that region and divides it the same way — into `~ceil(spanBytes / indexedSplitSize)` VFO splits (a single partition for a small region). This both skips bytes outside the region *and* parallelises a large region read across partitions. Spark post-filters the exact coordinate range; the reader guarantees the reference and the exact, duplicate-free union.

### When no BAI is present (BGZF block-range splitting)

An unindexed BAM is no longer read in a single partition — it is split into fixed-size byte chunks of `bgzfSplitSize` (default 128 MiB), producing `ceil(fileSize / bgzfSplitSize)` partitions. This count is **not** capped by `numPartitions`. The same path is taken when `useIndex=false`.

Chunk boundaries are computed arithmetically (`i × bgzfSplitSize`) rather than aligned to real BGZF block offsets. This is deliberate: enumerating block offsets would require the driver to scan the entire file sequentially — thousands of seeks on object storage — just to build the partition plan. Instead each executor orients itself locally.

```
unindexed sample.bam (600 MiB), bgzfSplitSize=128 MiB:
  Partition 0: bytes [0, 128M)    → first record ≥ byte 0
  Partition 1: bytes [128M, 256M) → first record ≥ byte 128M
  …
  Partition 4: bytes [512M, EOF)  → reads to end of file
```

A record's owning partition is the one whose byte range contains the start of the BGZF block holding the record's first byte, so each record is claimed exactly once. To begin, a partition must locate its first record start — which usually lies **inside** a BGZF block, because the writer flushes 64 KB blocks mid-record, so most blocks open with the tail of a straddling record. `BamPartitionReader.guessFirstRecordVfo` handles this: it walks the contiguous BGZF blocks from the chunk boundary (reading each block's header to get its compressed and uncompressed sizes, no full decode) and, within the first block, tries each uncompressed offset as a candidate record start. A candidate is accepted only when it and the following few records all parse as self-consistent BAM records (in-range `refID`/`pos`/`next_refID`/`next_pos` and a `block_size` consistent with the name/cigar/seq/qual lengths) — a run check that makes a coincidental mid-record match effectively impossible. The result is the exact record-start virtual file offset, so the split is lossless even when every block starts mid-record.

Setting `bgzfSplitSize` below the maximum BGZF block size (~65 KB) is counterproductive (many partitions then cover less than a block and yield zero rows), but it is not a correctness risk. The 128 MiB default suits typical WGS BAMs. For lower latency on large files, an indexed read (a co-located `.bai`) is still preferable — it seeks via exact BAI offsets instead of scanning for record starts.

---

## CRAM: container-level splitting

### When a CRAI is present

CRAI (CRAM index) files store per-container offset information. The scan reads the CRAI on the driver (a small gzip-compressed text file), deduplicates the container byte offsets, and distributes them into at most `numPartitions` contiguous groups. Each partition carries a `[start, end]` container byte-span that the reader passes to htsjdk's `CRAMIterator`, so executors decode only the containers in their assigned span.

### When a region filter is pushed

A single partition is planned with the pushed region; the reader issues a CRAI-guided query for `(ref, start, end)`, mirroring the BAM region-pushdown path.

### When no CRAI is present (container header scan)

An unindexed CRAM is split by container without an index. The driver makes one lightweight pass over the file reading only container *headers* via htsjdk's `CramContainerHeaderIterator` (no record decoding), collecting each container's byte offset. Those offsets are distributed into at most `numPartitions` groups exactly as in the CRAI case. Unlike unindexed BAM, this requires a sequential header pass on the driver — cheap, since only headers are read — and yields balanced, container-aligned partitions. The same path is taken when `useIndex=false`.

---

## SAM: line-based byte-range splitting

SAM is plain text with no BGZF framing and no index format, so blocks cannot be located by magic bytes. The file is divided into fixed-size chunks of `samSplitSize` (default 128 MiB), producing `ceil(fileSize / samSplitSize)` partitions (not capped by `numPartitions`). Each executor seeks to its chunk boundary, discards bytes up to the next newline to land on a clean line start, and parses SAM text lines (via `SAMLineParser`) until the next line would begin at or past its chunk end. Chunks containing no data lines produce zero rows. A pushed region filter cannot reduce I/O — there is no index — so Spark applies it as a post-filter over every partition's output.

---

## VCF: byte-range splitting (plain text) and tabix (bgzipped)

### Plain-text VCF (`.vcf`)

Plain-text VCF files use the same byte-range splitting strategy as uncompressed BED and SAM. The scan divides the file into fixed-size chunks (default 128 MiB, controlled by `vcfSplitSize`). Each executor seeks to its chunk boundary, scans forward to the next newline, reads the `#CHROM` header line from offset 0 to recover sample names, then parses data lines until its end boundary.

Because VCF records are fully self-contained per line (all eight mandatory columns plus per-sample genotype columns), the split is lossless: every record is read exactly once, by the partition whose range contains the start of that line.

```
plain calls.vcf (600 MiB), vcfSplitSize=128 MiB:
  Partition 0: bytes [0, 128M)    → reads header from offset 0, parses data lines in range
  Partition 1: bytes [128M, 256M) → reads header from offset 0, seeks to 128M
  Partition 2: bytes [256M, 384M) → …
  Partition 3: bytes [384M, 512M) → …
  Partition 4: bytes [512M, EOF)  → …
```

Any pushed region filter is applied by Spark as a post-filter over each partition's output. All partitions are read even when a `chrom` filter is present — bgzipped + tabix is more efficient for selective queries.

### Bgzipped VCF (`.vcf.gz`) and BCF

Tabix (`.tbi`, `.csi`) indexes store, for each chromosome block in a bgzipped file, the virtual file offsets of the BGZF blocks that contain records on that chromosome. The htsjdk `TabixIndex` API exposes these.

**When a tabix index is present and no region filter is pushed**, the scan reads the chromosome list from the index on the driver (by streaming the small BGZF-compressed index file via Hadoop FS) and groups chromosomes into at most `numPartitions` partitions. Each executor performs targeted tabix queries for its assigned chromosome group — mirroring the BAI VFO-based splitting used for BAM.

```
calls.vcf.gz (tabix, chr1–chr22 + chrX + chrY + chrM = 25 chroms), numPartitions=25:
  Partition 0:  chr1  → tabix query, reads only chr1 BGZF blocks
  Partition 1:  chr2  → tabix query, reads only chr2 BGZF blocks
  …
  Partition 24: chrM  → tabix query, reads only chrM BGZF blocks
```

**When a `chrom` filter (and optionally a `pos` range) is pushed down**, a single partition is planned that calls htsjdk's tabix query for the specified region — same targeted BGZF-block access, lower overhead than a full per-chromosome split.

**Without a tabix index**, a single full-file partition is planned.

## BED: byte-range splitting (plain text) and tabix (bgzipped)

Plain-text BED files (no `.gz` suffix) use byte-range splitting with a default split size of 128 MiB (controlled by `bedSplitSize`). Each executor seeks to its chunk start, skips to the next newline, and reads BED records until the end of its chunk. Header lines (`track`, `browser`, `#`) are skipped on every partition.

Bgzipped BED files (`.bed.gz`) use tabix-based partition planning when a co-located `.tbi` or `.csi` index is present — identical to bgzipped VCF.

---

## FASTA: FAI-based splitting

The FAI (FASTA index) format stores the byte offset of each contig's bases within the uncompressed FASTA file. The scan reads the FAI on the driver and creates one partition per contig. Each executor seeks to its contig's byte offset using `IndexedFastaSequenceFile.getSequence(name)` — no sequential scan.

---

## FASTQ: BGZF splitting and byte-range splitting

FASTQ has no standard seekable index. The split strategy depends on compression:

- **BGZF-compressed** (`.fastq.gz`/`.fq.gz` produced by `bgzip`, BCL2FASTQ, or most modern pipelines): the file is split into multiple partitions of at most `bgzfSplitSize` compressed bytes each (default 128 MiB). Each executor seeks to the first BGZF block at or after its partition boundary, then scans forward in the decompressed stream to the next `@` record.
- **Plain-gzip** (`.fastq.gz`/`.fq.gz` produced by standard `gzip`): a single partition covers the entire file, read sequentially by one executor.
- **Uncompressed** (`.fastq`/`.fq`): the file is divided into byte-range splits of at least 64 MB. Each executor scans forward from its split start to find the next `@` record boundary before beginning iteration.

---

## Predicate pushdown

The scan builders implement `SupportsPushDownFilters`. When Spark's query planner calls `pushFilters()`, the scan builder inspects the filter list for pushable patterns:

| Format | Pushable | Effect |
|---|---|---|
| BAM | `referenceName = 'X'` | BAI region query — reduces I/O |
| BAM | `referenceName = 'X' AND start >= A AND start <= B` | BAI region query with coordinate range — reduces I/O |
| VCF `.vcf.gz` | `chrom = 'X'` | Tabix query — reduces I/O |
| VCF `.vcf.gz` | `chrom = 'X' AND pos >= A AND pos <= B` | Tabix query with range — reduces I/O |
| VCF `.vcf` | `chrom = 'X'`, `pos` range | Spark post-filter only — all partitions are read |
| BED `.bed.gz` | `chrom = 'X'` | Tabix query — reduces I/O |
| BED `.bed.gz` | `chrom = 'X' AND chromStart >= A AND chromEnd <= B` | Tabix query with range — reduces I/O |
| BED `.bed` | `chrom = 'X'`, coordinate range | Spark post-filter only — all partitions are read |

**All pushed filters are returned as unhandled from `pushedFilters()`.** This means Spark re-applies every filter as a post-filter on the output rows regardless of format. For indexed formats (BAM, bgzipped VCF/BED), the pushdown effect additionally reduces which BGZF blocks are fetched from storage. For plain-text formats (uncompressed VCF, uncompressed BED), all byte-range partitions are read and Spark post-filters the rows — there is no I/O reduction, but the partitioning still provides read parallelism.

This design guarantees correctness for records near block boundaries without requiring the library to re-implement the boundary logic.

---

## Column pruning

The scan builders implement `SupportsPushDownRequiredColumns`. Spark calls `pruneColumns()` with the set of columns the query actually needs.

For BAM/SAM/CRAM, full `SAMRecord` objects must always be read from the file — records cannot be partially deserialized. The only optimization is **skipping the `attributes` map construction** when `attributes` is not in the required columns. Building the attributes map (parsing all optional SAM tags) has non-trivial CPU cost for records with many tags, so this skip is meaningful.

For other formats (VCF, BED, FASTA, FASTQ), column pruning records the required schema but does not currently skip per-field work at the reader level. Spark applies projection after rows are produced.

```python
# attributes map is NOT built per row (skipped at reader level)
df.select("readName", "referenceName", "start", "cigar").show()

# attributes map IS built (included in required schema)
df.select("readName", "attributes").show()
```

---

## Hadoop configuration propagation

Executors need the Hadoop `Configuration` (which contains S3A credentials, ADLS tokens, GCS service account keys, etc.) to open files on cloud storage. This configuration is captured on the driver as a `SerializableConfiguration` and stored inside each `InputPartition`. When Spark serializes the partition and sends it to an executor, the credentials travel with it.

This is why reads from `s3a://`, `abfss://`, or `gs://` paths work transparently on executors — the executor reconstructs the `FileSystem` from the serialized configuration using the same credentials that were present on the driver.

---

## numPartitions cap

`numPartitions` controls the maximum number of partitions per file (excluding the unmapped partition for BAM). The actual count is:

```
BAM (with BAI), refs ≤ numPartitions: one partition per reference (heavy refs sub-split by indexedSplitSize) + 1 unmapped
BAM (with BAI), refs > numPartitions: skew-aware — heavy refs sub-split, the rest bin-packed by byte span into the remaining budget + 1 unmapped
BAM (no BAI):                ceil(fileSize / bgzfSplitSize)        — not capped by numPartitions
SAM:                         ceil(fileSize / samSplitSize)         — not capped by numPartitions
CRAM (with CRAI):            min(numPartitions, numContainers)
CRAM (no CRAI):              min(numPartitions, numContainers)     — offsets from header scan
VCF .vcf.gz (tabix, no filter): min(numPartitions, numChroms)
VCF .vcf.gz (tabix, filter): 1 per pushed region query
VCF .vcf.gz (no index):      1 (full-file scan)
VCF .vcf (plain):            ceil(fileSize / vcfSplitSize)
BED .bed.gz (tabix):         1 per pushed region query
BED .bed (plain):            ceil(fileSize / bedSplitSize)
FASTA (with FAI):            min(numPartitions, numContigs)
FASTQ (BGZF):                min(numPartitions, ceil(fileSize / bgzfSplitSize))
FASTQ (plain-gzip):          1 (not splittable)
FASTQ (uncompressed):        min(numPartitions, ceil(fileSize / minSplitBytes))
```

Because a heavy reference is sub-split by `indexedSplitSize`, the actual BAM partition count can exceed `numPartitions` — the cap governs how references are *grouped*, not a hard ceiling once byte-sized splitting kicks in. Setting `numPartitions` higher than the number of references gives each reference its own partition (and sub-splits the heavy ones). Setting it lower than the reference count triggers the skew-aware grouping above: references are bin-packed by byte span rather than chopped into equal-count slices, so a hot reference is not merged with neighbours into a straggler.

`numPartitions` caps **index-guided** and container splits (BAM reference groups, CRAM container groups, tabix chromosome groups). It does **not** cap the byte-range splits of an unindexed BAM or a SAM file — those are governed solely by `bgzfSplitSize` / `samSplitSize`. To bound the partition count of a large unindexed BAM, raise `bgzfSplitSize`.

For a cohort of many small BAM files, the total partition count is the sum across all files. A directory of 100 BAM files with 25 references each and `numPartitions=200` would produce up to 2,600 partitions (100 files × 26 partitions each).
