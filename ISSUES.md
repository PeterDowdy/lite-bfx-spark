# Open Issues

Identified during PR #1 code review.

---

## Concerns

Items that are not blocking but deserve attention before the library stabilizes.

### ~~BamInputPartition is a 13-field mixed-mode struct~~ ✓ Resolved
Replaced the 6-deep constructor chain with seven named static factory methods
(`forFullScan`, `forRegionQuery`, `forVfoPartitions`, `forUnmapped`, `forBgzfSplit`,
`forSamSplit`, `forCramContainerSplit`). Each factory only accepts the fields its mode
uses; invalid combinations are no longer constructible. All call sites in `BamScan`,
`BamPartitionReaderTest`, and `RangeBamTest` updated. 97 BAM tests pass.

---

### ~~BGZF splits are not block-aligned~~ ✓ Documented
By design: enumerating block boundaries on the driver requires a full sequential file
scan (thousands of HTTP requests on cloud storage). Instead, each executor scans forward
from its arithmetic boundary with a single ~65 KB read — bounded, parallel, and
negligible at the default 128 MB split size. The tradeoff and the degenerate small-split
behaviour are now explained in the `planBgzfSplitPartitions` Javadoc.

**File:** `core/src/main/java/com/litebfx/bam/BamScan.java`

---

### VCF multi-sample genotype coverage is thin
`VcfTestGenerator` creates synthetic single-sample VCFs. Real-world VCFs with
complex `FORMAT` fields (`GT:AD:DP:GQ:PL`) and many samples are not exercised.
`VcfPartitionReader.buildGenotypesFromColumns` and `encodeGenotypeValue` are
covered by unit tests but not by any integration test using a realistic multi-sample
file. Add a multi-sample fixture to `VcfDataSourceTest`.

**Files:** `core/src/test/java/com/litebfx/vcf/VcfDataSourceTest.java`,
`core/src/test/java/com/litebfx/vcf/VcfTestGenerator.java`

---

### Glob and directory reads tested only for BAM
`BamDataSourceTest` covers glob patterns and directory scans. The equivalent paths
in `VcfScan` and `BedScan` have no integration-level tests for glob or directory
inputs. If `resolveFiles` / `resolveBamFiles` logic diverges between adapters,
the gap won't be caught.

**Files:** `core/src/test/java/com/litebfx/vcf/VcfDataSourceTest.java`,
`core/src/test/java/com/litebfx/bed/BedDataSourceTest.java`

---

## Gaps

Test coverage gaps identified by JaCoCo analysis (91.8% branch coverage) and manual
review. Items are grouped by difficulty to cover.

### Unreachable / structurally hard to cover

- **`BamPartitionReader` exception-injection paths (23 missed branches):** finally-block
  cleanup in `openBgzfSplit`, `openSamSplit`, `openCramContainerSplit`, and `open` is
  only reachable when the underlying Hadoop FS throws mid-open. Not coverable without
  mocking `FileSystem`.
- **`FastqPartitionReader.openBgzfSplit` failure-finally (3 branches):** same class of
  problem — `fs.open()` succeeds but the BGZF probe throws.
- **`FastaPartitionReader.next()` null/IllegalState branches (2 branches):**
  `refFile.getSequence()` throws `SAMException` instead of returning null, and the
  `refFile == null` guard in full-scan mode is unreachable because `open()` always sets
  exactly one reader.
- **`HadoopSeekableStream` TRACE guard (4 branches):** `isTraceEnabled()` true/false
  branches can't be covered in a single JaCoCo run.
- **IOException catch in `VcfScan`, `FastaScan`, `BedScan` `planInputPartitions`
  (3 branches):** require a FS network failure to trigger.

---

### Achievable — missing fixtures or test cases

- **CRAM with external FASTA reference:** `referenceMode=file` code path exists but
  is never exercised with an actual decode. Add a test using a real CRAM + matching
  FASTA reference file.
  **File:** `core/src/test/java/com/litebfx/bam/CramDataSourceTest.java`

- **BED tabix multi-block exhaustion:** `blockIterator.hasNext()` returning `false`
  after at least one block is read has no test. Need a BED+TBI fixture whose query
  region spans at least two tabix index blocks.
  **File:** `core/src/test/java/com/litebfx/bed/BedPartitionReaderTest.java`

- **VCF multi-sample genotype parsing:** `buildGenotypesFromColumns` and
  `encodeGenotypeValue` are unit-tested but no integration test uses a realistic
  multi-sample VCF with `GT:AD:DP:GQ:PL` FORMAT fields.
  **File:** `core/src/test/java/com/litebfx/vcf/VcfDataSourceTest.java`

- **Glob and directory reads for VCF and BED:** covered for BAM, missing for the
  other two adapters.
  **Files:** `core/src/test/java/com/litebfx/vcf/VcfDataSourceTest.java`,
  `core/src/test/java/com/litebfx/bed/BedDataSourceTest.java`

- **`FastqScan.resolveFiles` edge cases (8 missed branches):** `globStatus()` returns
  null, glob returns only non-FASTQ files, empty file list after filtering,
  directory-vs-file fallback. Requires crafting a mock or unusual directory layout.
  **File:** `core/src/test/java/com/litebfx/fastq/FastqDataSourceTest.java`

- **`BamScan` partition-planning edge cases (20 missed branches):** empty CRAI, no
  containers found in CRAM header scan, split-size boundary conditions, multi-file
  VFO splitting fallback. Many require crafted/degenerate BAM/CRAM files.
  **File:** `core/src/test/java/com/litebfx/bam/BamDataSourceTest.java`

- **BGZF magic partial-match bytes** in both `BamPartitionReader` and
  `FastqPartitionReader` `findNextBgzfBlockStart` (4 missed branches total): require
  raw bytes `0x1F` followed by non-magic bytes inside compressed BGZF data. Not
  deterministically producible without patching file bytes.

---

## High Priority

### ~~VCF cloud URI limitation~~ ✓ Resolved
Added a **BGZF Hadoop path** to `VcfPartitionReader` that activates for any non-local
URI (`s3a://`, `gs://`, `wasb://`, `hdfs://`, etc.). Opens the `.vcf.gz` file via
Hadoop `FileSystem`, wraps it in `BlockCompressedInputStream` via `HadoopSeekableStream`,
parses the VCF header for sample names, loads the tabix index via Hadoop FS when
available, and seeks to the relevant BGZF blocks for per-chromosome or single-region
queries. Falls back to sequential full-scan when no tabix index is present.
Local paths continue to use `VCFFileReader` (which also handles BCF). BCF on remote
paths throws `UnsupportedOperationException` with a helpful message. 115 VCF tests pass.

**File:** `core/src/main/java/com/litebfx/vcf/VcfPartitionReader.java`

---

### `start` / `mateStart` IntegerType overflow
Both BAM position fields are declared `IntegerType` in `BamSchema`, but SAM positions
can technically exceed `Integer.MAX_VALUE` for very large chromosomes (e.g., chromosome-
scale assemblies or synthetic references). `LongType` would be safer and is consistent
with how BED uses `LongType` for `chromStart`/`chromEnd`.

**File:** `core/src/main/java/com/litebfx/bam/BamSchema.java`

---

## Medium Priority

### `staging-publish` job missing credentials guard
The `staging-publish` CI job runs on every push to `main` with no guard on whether the
required secrets (`OSSRH_USERNAME`, `OSSRH_TOKEN`, `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`)
are configured. Every merge will trigger a deploy attempt that fails if the secrets are
absent. Add an `if:` condition or document that these secrets must be set before any
merge to `main`.

**File:** `.github/workflows/ci.yml` — `staging-publish` job

---

### `alt` should be `ArrayType(StringType)`
`alt` in `VcfSchema` is `StringType`, which collapses multi-allelic sites (e.g.,
`A,T,G`) into a single comma-joined string. Downstream code must split on comma to
work with individual alleles, and allele-specific DataFrame filters are awkward.
`ArrayType(StringType)` is more Spark-idiomatic. This is a breaking schema change —
do it before the first release.

**File:** `core/src/main/java/com/litebfx/vcf/VcfSchema.java`

---

### Cloud integration CI job has no timeout
If MinIO or fake-gcs-server fail to start (port conflict, image pull failure, etc.),
the `until` health-check loops in the cloud startup steps will spin indefinitely and
the job will never complete. Add `timeout-minutes: 20` to the `cloud-integration` job.

**File:** `.github/workflows/ci.yml` — `cloud-integration` job

---

### Coordinate system mismatch undocumented at point of use
BAM `start` is 1-based (SAM spec); BED `chromStart` is 0-based. Joining BAM and BED
DataFrames on genomic coordinates silently produces wrong results without an off-by-one
correction. The warning exists in `BamSchema` Javadoc but not in `docs/bam.md` or the
README where users are most likely to see it. Promote the caveat to those locations and
consider adding a 0-based `start0` computed column to `BamSchema`.

**Files:** `core/src/main/java/com/litebfx/bam/BamSchema.java`, `docs/bam.md`, `README.md`

---

## Low Priority

### Remove `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` env var
This global env var was a workaround to silence Node 20 deprecation warnings.
All actions have now been upgraded to Node 24 natively (`checkout@v6`, `setup-java@v5`,
`upload-artifact@v7`), so the var is no longer needed and should be removed to avoid
confusion.

**File:** `.github/workflows/ci.yml` — top-level `env:` block

---

### Azurite well-known key will trip secret scanners
The public Azurite development key (`Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSR...`)
is hardcoded in `core/pom.xml` and three Azure test files. It is not a real secret,
but GitHub secret scanning and tools like truffleHog will flag it. Add an inline
suppression comment (`# pragma: allowlist secret` or `# gitleaks:allow`) and/or
consolidate the key into a single constant in a shared test helper.

**Files:**
- `core/pom.xml` — `azure.key` property
- `core/src/test-azure/java/com/litebfx/bam/BamAzureTest.java`
- `core/src/test-azure/java/com/litebfx/fasta/FastaAzureTest.java`
- `core/src/test-azure/java/com/litebfx/vcf/VcfAzureTest.java`

---

### Large FASTQ fixtures committed directly (no Git LFS)
The four `*.fastq.gz` test fixtures total ~7.5 MB and are committed directly to the
repo. This slows `git clone` and `git fetch` for all contributors. Move them to Git LFS
(`git lfs track "*.fastq.gz"`) or generate them programmatically at test time using the
same `TestBamGenerator` / `FastqTestGenerator` pattern used for BAM and other formats.

**Files:** `core/src/test/resources/TESTX_H7YRLADXX_*.fastq.gz` (4 files)

---

### BED tabix multi-block path untested
The `BedPartitionReader.next()` branch where `blockIterator.hasNext()` returns `false`
(i.e., the iterator is exhausted after yielding at least one block) has no test. This
means the tabix region-query path that spans multiple index blocks is only partially
exercised. Add a test using a BED+TBI fixture whose query region spans two tabix blocks.

**File:** `core/src/test/java/com/litebfx/bed/BedPartitionReaderTest.java`

---

### CRAM with external reference untested
The code path for `referenceMode=file` (CRAM decoding against a FASTA reference file)
is present but has no test that actually decodes records. Only the empty-spans edge case
is covered. Add a test using a real CRAM + FASTA reference pair.

**File:** `core/src/test/java/com/litebfx/bam/CramDataSourceTest.java`
