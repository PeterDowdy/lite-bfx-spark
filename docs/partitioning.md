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

## VCF and BED: tabix-based splitting

Tabix (`.tbi`, `.csi`) indexes store, for each chromosome block in a bgzipped file, the virtual file offsets of the BGZF blocks that contain records on that chromosome. The htsjdk `TabixIndex` API exposes these.

When a `chrom` filter (and optionally a `pos`/`chromStart`/`chromEnd` range) is pushed down, the scan calls `TabixIndex.getBlocks(chrom, start, end)` to get the VFO spans for the query region. Currently a single partition is planned per query; the partition's byte range covers all relevant BGZF blocks.

Without a tabix index, or when no region filter is pushed, a single full-file partition is planned.

---

## FASTA: FAI-based splitting

The FAI (FASTA index) format stores the byte offset of each contig's bases within the uncompressed FASTA file. The scan reads the FAI on the driver and creates one partition per contig. Each executor seeks to its contig's byte offset using `IndexedFastaSequenceFile.getSequence(name)` — no sequential scan.

---

## FASTQ: byte-range splitting

FASTQ has no standard seekable index. For uncompressed files the scan divides the file into byte-range splits of at least 64 MB. Each executor scans forward from its split start to find the next `@` record boundary before beginning iteration. For gzipped files, a single partition is always used.

---

## Predicate pushdown

The scan builders implement `SupportsPushDownFilters`. When Spark's query planner calls `pushFilters()`, the scan builder inspects the filter list for pushable patterns:

| Format | Pushable | Effect |
|---|---|---|
| BAM | `referenceName = 'X'` | BAI region query |
| BAM | `referenceName = 'X' AND start >= A AND start <= B` | BAI region query with coordinate range |
| VCF | `chrom = 'X'` | Tabix query |
| VCF | `chrom = 'X' AND pos >= A AND pos <= B` | Tabix query with range |
| BED | `chrom = 'X'` | Tabix query |
| BED | `chrom = 'X' AND chromStart >= A AND chromEnd <= B` | Tabix query with range |

**All pushed filters are returned as unhandled from `pushedFilters()`.** This means Spark re-applies every filter as a post-filter on the output rows. The pushdown effect is purely at the partition-planning level (which BGZF blocks to read), not at the row-filtering level. This is intentional: it guarantees correctness for records near block boundaries without requiring the library to re-implement the boundary logic.

The practical effect is: pushed filters reduce I/O (fewer BGZF blocks are fetched from object storage), and Spark's post-filter reduces the rows returned to the query. Both happen automatically.

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
BAM (with BAI):  min(numPartitions, numReferences) + 1 unmapped partition
CRAM (with CRAI): number of CRAI entries (not capped — each container is one partition)
VCF/BED (tabix): 1 per pushed region query (numPartitions reserved for future use)
FASTA (with FAI): min(numPartitions, numContigs)
FASTQ (uncompressed): min(numPartitions, ceil(fileSize / 64MB))
```

Setting `numPartitions` higher than the number of references in a BAM has no effect — you cannot have more partitions than references. Setting it lower groups references together, reducing parallelism but also reducing the number of BAI seeks and task overhead.

For a cohort of many small BAM files, the total partition count is the sum across all files. A directory of 100 BAM files with 25 references each and `numPartitions=200` would produce up to 2,600 partitions (100 files × 26 partitions each).
