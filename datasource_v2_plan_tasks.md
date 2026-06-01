# DataSource V2 Implementation Tasks

Companion checklist to `datasource_v2_plan.md`. Work top-to-bottom; each phase leaves
the codebase in a passing state. All tests run via Docker — see `TESTING.md`.

---

## Phase 1 — Foundation refactors

No behavior change. Prerequisite for Phases 2–4.

### 1.1 Extract `resolveFiles()` from each `*Scan`

Signature: `static List<FileStatus> resolveFiles(CaseInsensitiveStringMap options, Configuration conf) throws IOException`  
Body: the glob/directory expansion currently inlined at the top of each `planInputPartitions()`. Leave `planInputPartitions()` calling the new helper.

- [x] `BamScan`: extract `resolveFiles()`, replace inline block in `planInputPartitions()` with the call
- [x] `VcfScan`: same
- [x] `BedScan`: same
- [x] `FastaScan`: same
- [x] `FastqScan`: same

### 1.2 Extract `ensureHeaderRead()` in `BamScan`

- [x] Add `private boolean headerRead = false` and `private boolean isCoordinateSorted = false` fields to `BamScan`
- [x] Extract a `private void ensureHeaderRead(Configuration conf)` method that opens the first resolved file with `SamReaderFactory`, reads the `SAMFileHeader`, sets `isCoordinateSorted = header.getSortOrder() == SAMFileHeader.SortOrder.coordinate`, then closes the reader; guards with `if (headerRead) return;`
- [x] Call `ensureHeaderRead(conf)` at the top of `planInputPartitions()` (before the existing header open, which can be removed or reused)

---

## Phase 2 — Statistics (`SupportsReportStatistics`)

Add to the `*Scan` class for each format. Requires Phase 1 complete.

### 2.1 FASTQ

- [x] Add `implements SupportsReportStatistics` to `FastqScan`
- [x] Add import `org.apache.spark.sql.connector.read.SupportsReportStatistics` and `org.apache.spark.sql.connector.read.Statistics`
- [x] Implement `estimateStatistics()`: call `resolveFiles()`, sum `FileStatus.getLen()`, return `Statistics` with `sizeInBytes = OptionalLong.of(total)` and `numRows = OptionalLong.empty()`; return both empty on `IOException`
- [x] Add test in `FastqDataSourceTest`: load a FASTQ file, assert `df.queryExecution().optimizedPlan().stats().sizeInBytes().longValue() > 0`

### 2.2 FASTA

- [x] Add `private final int faiEntryCount` field to `FastaScan` (−1 = unknown)
- [x] Update both `FastaScan` constructors to accept `int faiEntryCount`; the no-arg default passes −1
- [x] Update `FastaScanBuilder.build()` to pass the FAI contig count (the FAI line count already read during `pushFilters`) to the `FastaScan` constructor; pass −1 if no FAI was resolved
- [x] Add `implements SupportsReportStatistics` to `FastaScan`
- [x] Implement `estimateStatistics()`: `sizeInBytes` from `resolveFiles()` sum; `numRows = faiEntryCount >= 0 ? OptionalLong.of(faiEntryCount) : OptionalLong.empty()`
- [x] Add test in `FastaDataSourceTest`: load an indexed FASTA, assert `sizeInBytes > 0` and `numRows == expectedContigCount`

### 2.3 BAM/CRAM

- [x] Add `implements SupportsReportStatistics` to `BamScan`
- [x] Implement `estimateStatistics()`: call `resolveFiles()`, sum sizes, return `sizeInBytes` only (`numRows` empty)
- [x] Add test in `BamDataSourceTest`: load the test BAM, assert `sizeInBytes > 0` and `sizeInBytes <= actualFileSize * 2`
- [x] Add same assertion in `CramDataSourceTest`

### 2.4 VCF

- [x] Add `implements SupportsReportStatistics` to `VcfScan`
- [x] Implement `estimateStatistics()` same as BAM pattern
- [x] Add test in `VcfDataSourceTest`

### 2.5 BED

- [x] Add `implements SupportsReportStatistics` to `BedScan`
- [x] Implement `estimateStatistics()` same as BAM pattern
- [x] Add test in `BedDataSourceTest`

---

## Phase 3 — Limit pushdown (`SupportsPushDownLimit`)

Four files per format. Requires Phase 1 complete.

### 3.1 FASTQ

**`FastqScanBuilder`**
- [x] Add `implements SupportsPushDownLimit`
- [x] Add import `org.apache.spark.sql.connector.read.SupportsPushDownLimit`
- [x] Add field `private int pushedLimit = Integer.MAX_VALUE`
- [x] Implement `pushLimit(int limit)`: set `this.pushedLimit = limit; return false;`
- [x] Update `build()` to pass `pushedLimit` to `FastqScan` constructor

**`FastqScan`**
- [x] Add `private final int pushedLimit` field and accept it in the constructor
- [x] In `planInputPartitions()`: when `pushedLimit < Integer.MAX_VALUE`, plan only the first split (skip remaining splits) and set its `rowLimit` to `pushedLimit`

**`FastqInputPartition`**
- [x] Add `private final int rowLimit` field (default `Integer.MAX_VALUE`)
- [x] Add constructor overload or factory parameter to accept `rowLimit`
- [x] Add `public int rowLimit()` accessor

**`FastqPartitionReader`**
- [x] Add `private long rowsRead = 0` field
- [x] In `next()`: add `if (rowsRead >= partition.rowLimit()) return false;` as the first check
- [x] Increment `rowsRead++` after a successful record advance (before returning `true`)

**Test**
- [x] Add test in `FastqDataSourceTest`: `.load().limit(5)`, assert `count() == 5` and `rdd().getNumPartitions() == 1`

### 3.2 BAM/CRAM

**`BamScanBuilder`**
- [x] Add `implements SupportsPushDownLimit`
- [x] Add field `private int pushedLimit = Integer.MAX_VALUE`
- [x] Implement `pushLimit(int limit)`: `this.pushedLimit = limit; return false;`
- [x] Update `build()` to pass `pushedLimit` to `BamScan`

**`BamScan`**
- [x] Add `private final int pushedLimit` field and accept in constructor
- [x] In `planInputPartitions()`: when `pushedLimit < Integer.MAX_VALUE` and no region was pushed, plan only the first one or two partitions (first reference + unmapped) instead of all references; set `rowLimit = pushedLimit` on those partitions

**`BamInputPartition`**
- [x] Add `private final int rowLimit` field (default `Integer.MAX_VALUE`) to the shared fields block
- [x] Thread `rowLimit` through all factory methods (`forFullScan`, `forRegionQuery`, `forVfoPartitions`, etc.) with a default of `Integer.MAX_VALUE` so existing call sites are unchanged
- [x] Add `public int rowLimit()` accessor

**`BamPartitionReader`**
- [x] Add `private long rowsRead = 0` field
- [x] In `next()`: add `if (rowsRead >= partition.rowLimit()) return false;` as the first check
- [x] Increment `rowsRead++` after advancing the SAM iterator (before returning `true`)

**Tests**
- [x] Add test in `BamDataSourceTest`: `.load().limit(3)`, assert `count() == 3`
- [x] Add test: region filter + `limit(2)` — assert correctness, region filter still applied
- [x] Add same limit test in `CramDataSourceTest`

### 3.3 VCF

**`VcfScanBuilder`**
- [x] Add `implements SupportsPushDownLimit`, field `private int pushedLimit = Integer.MAX_VALUE`
- [x] Implement `pushLimit(int limit)`: set field, `return false`
- [x] Update `build()` to pass `pushedLimit` to `VcfScan`

**`VcfScan`**
- [x] Accept and store `pushedLimit`; when `< Integer.MAX_VALUE` and no region pushed, plan only the first partition

**`VcfInputPartition`**
- [x] Add `rowLimit` field (default `Integer.MAX_VALUE`) and accessor

**`VcfPartitionReader`**
- [x] Add `rowsRead` counter; check against `partition.rowLimit()` in `next()`

**Test**
- [x] Add test in `VcfDataSourceTest`: `.load().limit(5)`, assert `count() == 5`
- [x] Add test: chrom filter + `limit(2)`, assert correctness

### 3.4 BED

**`BedScanBuilder`**
- [x] Add `implements SupportsPushDownLimit`, field `private int pushedLimit = Integer.MAX_VALUE`
- [x] Implement `pushLimit(int limit)`: set field, `return false`
- [x] Update `build()` to pass `pushedLimit` to `BedScan`

**`BedScan`**
- [x] Accept and store `pushedLimit`; when `< Integer.MAX_VALUE` and no region pushed, plan only the first partition

**`BedInputPartition`**
- [x] Add `rowLimit` field (default `Integer.MAX_VALUE`) and accessor

**`BedPartitionReader`**
- [x] Add `rowsRead` counter; check against `partition.rowLimit()` in `next()`

**Test**
- [x] Add test in `BedDataSourceTest`: `.load().limit(5)`, assert `count() == 5`

### 3.5 FASTA

**`FastaScanBuilder`**
- [x] Add `implements SupportsPushDownLimit`, field `private int pushedLimit = Integer.MAX_VALUE`
- [x] Implement `pushLimit(int limit)`: set field, `return false`
- [x] Update `build()` to pass `pushedLimit` to `FastaScan`

**`FastaScan`**
- [x] Accept and store `pushedLimit`; when `pushedLimit == 1` and N contig partitions would be planned, plan only the first

**`FastaInputPartition`**
- [x] Add `rowLimit` field (default `Integer.MAX_VALUE`) and accessor

**`FastaPartitionReader`**
- [x] Add `rowsRead` counter; check against `partition.rowLimit()` in `next()`

**Test**
- [x] Add test in `FastaDataSourceTest`: `.load().limit(1)`, assert `count() == 1`

---

## Phase 4 — Ordering (`SupportsReportOrdering`)

Requires Phase 1.2 (`ensureHeaderRead`) complete for BAM. VCF and BED are independent.

### 4.1 VCF

- [x] Add `private boolean hasTabixIndex = false` field to `VcfScan`
- [x] In `planInputPartitions()`, after the tabix index resolution block, set `hasTabixIndex = true` when a tabix path was found
- [x] Add `implements SupportsReportOrdering` to `VcfScan`
- [x] Add imports: `org.apache.spark.sql.connector.expressions.SortOrder`, `org.apache.spark.sql.connector.expressions.FieldReference`, `org.apache.spark.sql.connector.expressions.SortDirection`, `org.apache.spark.sql.connector.expressions.NullOrdering`
- [x] Implement `outputOrdering()`: resolves index lazily; returns `[chrom ASC NULLS LAST, pos ASC NULLS LAST]` when tabix index found, empty otherwise
- [x] Add test in `VcfDataSourceTest`: tabix-indexed VCF → ordering `[chrom, pos]`
- [x] Add test: plain VCF without index → empty ordering

### 4.2 BED

- [x] Add `private boolean hasTabixIndex = false` field to `BedScan`
- [x] In `planInputPartitions()`, set `hasTabixIndex = true` when tabix path is resolved
- [x] Add `implements SupportsReportOrdering` to `BedScan`
- [x] Implement `outputOrdering()`: resolves index lazily; returns `[chrom ASC NULLS LAST, chromStart ASC NULLS LAST]` when tabix index found, empty otherwise
- [x] Add test in `BedDataSourceTest`: indexed file → non-empty ordering matching `[chrom, chromStart]`; unindexed file → empty ordering

### 4.3 BAM/CRAM

- [x] `BamScan`: call `ensureHeaderRead(conf)` in `planInputPartitions()` (already present)
- [x] In `ensureHeaderRead()`: also check index existence; set `isCoordinateSorted = false` when no index present
- [x] Add `implements SupportsReportOrdering` to `BamScan`
- [x] Implement `outputOrdering()`: calls `ensureHeaderRead()`; returns `[referenceName ASC NULLS LAST, start ASC NULLS LAST]` when `isCoordinateSorted`, empty otherwise
- [x] Add test in `BamDataSourceTest`: BAM + BAI → non-empty ordering with `[referenceName, start]`
- [x] Add test: SAM file (no index) → empty ordering
- [x] Add test: BAM with `SO:queryname` header → empty ordering; added `TestBamGenerator.generateQuerynameSortedBam()`
- [x] Add same ordering presence/absence tests in `CramDataSourceTest`

---

## Phase 5 — V2 predicate pushdown (`SupportsPushDownV2Filters`)

Replace V1 `SupportsPushDownFilters` entirely. One format at a time.

### 5.1 FASTA

**`FastaScanBuilder`**
- [x] Remove import `org.apache.spark.sql.sources.EqualTo` and `org.apache.spark.sql.sources.Filter`
- [x] Remove `SupportsPushDownFilters` from `implements` clause and its import
- [x] Add `implements SupportsPushDownV2Filters` and import `org.apache.spark.sql.connector.read.SupportsPushDownV2Filters`
- [x] Add imports: `org.apache.spark.sql.connector.expressions.filter.Predicate`, `org.apache.spark.sql.connector.expressions.Literal`, `org.apache.spark.sql.connector.expressions.NamedReference`
- [x] Add field `private Predicate[] handledPredicates = new Predicate[0]`
- [x] Add public static helpers `isColumnEquality`, `isColumnEqualityIgnoreCase`, `isRangeComparison`, `literalValue`, `columnName`, `flatten` for cross-package reuse
- [x] Replace `pushFilters(Filter[])` with `pushPredicates(Predicate[])`: flatten `And` trees, match `"="` predicates with `fieldNames()[0].equals("name")`, extract literal value into `pushedName`; set `handledPredicates`; return unhandled
- [x] Replace `pushedFilters()` with `pushedPredicates()` returning `handledPredicates`
- [x] Update `FastaScanBuilderTest`: replace V1 predicate construction with V2 `new Predicate("=", ...)` + `LiteralValue.apply()`; test `And`-unwrapping; assert on `pushedPredicates().length`

### 5.2 VCF

**`VcfScanBuilder`**
- [x] Remove all `org.apache.spark.sql.sources.*` imports and `SupportsPushDownFilters` import/interface
- [x] Add `SupportsPushDownV2Filters` and V2 predicate imports; delegate range matching to `FastaScanBuilder` helpers
- [x] Add `private Predicate[] handledPredicates = new Predicate[0]` field
- [x] Replace `pushFilters()` with `pushPredicates()`: first pass extracts `chrom` from `"="` predicate; second pass (only when `chrom` found) extracts `pos` range from `">"`, `">="`, `"<"`, `"<="` predicates; return unhandled, set `handledPredicates`
- [x] Replace `pushedFilters()` with `pushedPredicates()`
- [x] Update `VcfScanBuilderTest`: V2 predicate construction; test `And(eq("chrom","chr1"), gte("pos",1000))` unwrapping; assert handled/unhandled counts

### 5.3 BED

**`BedScanBuilder`**
- [x] Remove V1 imports and interface; add V2 imports and interface; delegate to `FastaScanBuilder` helpers
- [x] Add `handledPredicates` field
- [x] Replace `pushFilters()` with `pushPredicates()`: same two-pass pattern; equality on `chrom`, case-insensitive range on `chromStart` (lower) and `chromEnd` (upper)
- [x] Replace `pushedFilters()` with `pushedPredicates()`
- [x] Update `BedScanBuilderTest`: V2 predicate construction; test case-insensitive `chromstart`/`chromend`; test `chromEnd` upper-bound predicate; test `And`-unwrapping

### 5.4 BAM/CRAM

**`BamScanBuilder`**
- [x] Remove V1 imports and interface; add V2 imports and interface; delegate to `FastaScanBuilder` helpers
- [x] Add `handledPredicates` field
- [x] Replace `pushFilters()` with `pushPredicates()`: case-insensitive equality on `referenceName`, case-insensitive range on `start`; two-pass structure; return unhandled; set `handledPredicates`
- [x] Replace `pushedFilters()` with `pushedPredicates()`
- [x] Update `BamScanBuilderTest`: V2 predicate construction; test `referencename` (lowercase) case-insensitive matching; test `And(eq("referenceName","chr1"), gte("start",100L))` unwrapping
- [x] Verify `CramDataSource` is unaffected (it reuses `BamScanBuilder`; no separate ScanBuilder to change)

---

## Final verification

- [x] Run full test suite: `docker compose run --rm spark-test mvn test -Pspark402` — 467 tests, 0 failures
- [x] Run against Spark 4.1.1: `docker compose run --rm spark411 mvn test -Pspark411` — 467 tests, 0 failures
- [x] Run S3 range tests: `docker compose run --rm spark-test-s3` — 476 tests, 1 expected skip (BedS3Test assumeTrue), 0 failures
- [x] Confirm `EXPLAIN` output on a filtered BAM query shows pushed predicates — two tests added to `BamDataSourceTest`: `explain_referenceNameEquality_removedFromPhysicalPlan` (pushed equality absent from executed plan) and `explain_startRange_appearsAsPostScanFilter` (unhandled range visible as post-scan Filter node)
