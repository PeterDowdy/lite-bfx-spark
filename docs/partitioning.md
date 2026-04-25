# Partitioning and indexes

This document explains how lite-bfx-spark plans Spark partitions, uses genomics indexes for partition pruning, and interacts with Spark's filter and projection pushdown mechanisms.

---

## Overview

A Spark partition in this library is a `(filePath, byteRange, queryParams, hadoopConf)` tuple. Each executor opens the file at the given path, seeks to the byte range using the Hadoop `FileSystem`, and iterates records until the range is exhausted. The planning of these partitions — how many there are, which byte ranges they cover — is driven by the genomics index for each format.

The core insight is that genomics index formats (BAI, CRAI, tabix) map genomic coordinates to **virtual file offsets (VFOs)** within the BGZF-compressed file. By reading the index on the driver, the scan planner can translate a genomic query into a set of byte ranges, one per Spark partition, without reading any alignment data.

---

## BAM: VFO-based splitting with BAI

BAI (BAM index) files store, for each reference sequence, the set of BGZF block offsets that contain alignments on that reference. The htsjdk `BAMFileSpan` API exposes these as virtual file offset (VFO) chunks.

### When a BAI is present and no region filter is pushed

The scan reads the BAM header on the driver to enumerate all reference sequences. It then groups references into partitions (up to `numPartitions`, defaulting to 200) and creates one partition per group plus one partition for unmapped reads.

Each per-reference partition receives a `QueryInterval[]` covering its assigned reference sequences. When the executor opens the partition, htsjdk uses the BAI to seek directly to the BGZF blocks that contain those references' reads — no full-file scan occurs.

```
BAM file: chr1 chr2 chr3 chr4 … chr22 chrX chrY chrM
numPartitions=5:
  Partition 0: [chr1, chr2, chr3, chr4, chr5]  → BAI VFO seek for all 5 refs
  Partition 1: [chr6, chr7, …, chr10]
  Partition 2: [chr11, …, chr15]
  Partition 3: [chr16, …, chr22, chrX, chrY, chrM]
  Partition 4: [unmapped reads]                 → samReader.queryUnmapped()
```

Total records across all partitions = all records in the file.

### When a region filter is pushed

When the query planner receives a filter like `referenceName = 'chr17' AND start >= 43044295 AND start <= 43125370`, the scan produces **one partition** containing a BAI-guided region query. The executor calls `samReader.query(ref, start, end, false)` which uses the BAI to seek directly to the relevant BGZF blocks.

VFO-based splitting is disabled when a region filter is active — there is no benefit to splitting across references when only one region is requested.

### When no BAI is present

A single partition covering `[0, Long.MAX_VALUE]` is created. The executor reads the entire BAM file sequentially.

---

## CRAM: CRAI-based splitting

CRAI (CRAM index) files store per-container offset information. The splitting logic mirrors the BAI approach: the scan reads CRAI entries on the driver and maps them to `BamInputPartition` byte ranges. Executors use htsjdk's CRAM reader with the pre-computed VFOs to skip to the relevant containers.

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
BAM (with BAI):              min(numPartitions, numReferences) + 1 unmapped partition
CRAM (with CRAI):            number of CRAI entries (not capped — each container is one partition)
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

Setting `numPartitions` higher than the number of references in a BAM has no effect — you cannot have more partitions than references. Setting it lower groups references together, reducing parallelism but also reducing the number of BAI seeks and task overhead.

For a cohort of many small BAM files, the total partition count is the sum across all files. A directory of 100 BAM files with 25 references each and `numPartitions=200` would produce up to 2,600 partitions (100 files × 26 partitions each).
