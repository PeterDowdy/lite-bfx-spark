# DataSource V2 Enhancement Plan

Adds four DataSource V2 capabilities to all formats in lite-bfx-spark.
Written against the current codebase on the `first_release_fixes` branch.

---

## Scope

| Feature | API | Formats |
|---|---|---|
| 1. V2 predicate pushdown | `SupportsPushDownV2Filters` | BAM/CRAM, VCF, BED, FASTA |
| 2. Statistics reporting | `SupportsReportStatistics` | All five formats |
| 3. Ordering reporting | `SupportsReportOrdering` | BAM/CRAM, VCF, BED |
| 4. Limit pushdown | `SupportsPushDownLimit` | All five formats |

FASTQ has no index and no sort order, so it participates in statistics and limit only.

---

## Feature 1: `SupportsPushDownV2Filters`

### Why

The existing `SupportsPushDownFilters` (V1) uses `org.apache.spark.sql.sources.Filter`,
which cannot represent `NOT`, `IS NULL / IS NOT NULL`, nested struct predicates, or
decimal/timestamp literals. `SupportsPushDownV2Filters` uses
`org.apache.spark.sql.connector.expressions.filter.Predicate` — a richer,
composable tree. Spark prefers V2 when both are present; migrating fully eliminates
the V1 code path.

### What changes

Replace `SupportsPushDownFilters` with `SupportsPushDownV2Filters` in all four
ScanBuilder classes. FASTQ has no pushdown at all and is unaffected.

**New interface:**
```java
// org.apache.spark.sql.connector.read
interface SupportsPushDownV2Filters {
    Predicate[] pushPredicates(Predicate[] predicates);  // return unhandled
    Predicate[] pushedPredicates();                       // return handled (for EXPLAIN)
}
```

**V2 predicate types to handle** (from `org.apache.spark.sql.connector.expressions.filter`):
- `EqualTo(NamedReference ref, Literal<?> value)`
- `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`
- `And` — recurse into both children
- `Not(EqualTo)` — can be used to assert non-null contig, skip

**V1 types being replaced** (from `org.apache.spark.sql.sources`):
- `EqualTo`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`

### Format-specific field mappings

| Format | Equality field | Range field(s) |
|---|---|---|
| BAM/CRAM | `referenceName` | `start` (LongType, 1-based) |
| VCF | `chrom` | `pos` (IntegerType, 1-based) |
| BED | `chrom` | `chromStart` (LongType, 0-based), `chromEnd` (LongType, 0-based) |
| FASTA | `name` | — (equality only; no range pushdown) |

### Files to change per format

**BAM/CRAM — `BamScanBuilder.java`**
1. Remove imports: `org.apache.spark.sql.sources.*` (EqualTo, Filter, etc.)
2. Add imports: `org.apache.spark.sql.connector.expressions.filter.*`,
   `org.apache.spark.sql.connector.expressions.Literal`,
   `org.apache.spark.sql.connector.expressions.NamedReference`
3. Change `implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns`
   to `implements ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns`
4. Replace `pushFilters(Filter[])` + `pushedFilters()` with:

```java
private Predicate[] handledPredicates = new Predicate[0];

@Override
public Predicate[] pushPredicates(Predicate[] predicates) {
    List<Predicate> unhandled = new ArrayList<>();
    List<Predicate> handled   = new ArrayList<>();
    String refName = null;
    int rangeStart = 1, rangeEnd = Integer.MAX_VALUE;

    // First pass: extract referenceName equality
    for (Predicate p : predicates) {
        if (p instanceof EqualTo eq
                && fieldName(eq.references()).equalsIgnoreCase("referenceName")
                && eq.value() instanceof Literal<?> lit) {
            refName = String.valueOf(lit.value());
        }
    }
    // Second pass: extract start range only when referenceName was found
    if (refName != null) {
        for (Predicate p : predicates) {
            String attr = fieldName(p.references());
            if (p instanceof GreaterThanOrEqual gte
                    && attr.equalsIgnoreCase("start")
                    && gte.value() instanceof Literal<?> lit) {
                rangeStart = ((Number) lit.value()).intValue();
                handled.add(p); continue;
            }
            // ... GreaterThan, LessThanOrEqual, LessThan  (same pattern)
            unhandled.add(p);
        }
        pushedReferenceName = refName;
        pushedStart = rangeStart;
        pushedEnd   = rangeEnd;
    } else {
        Collections.addAll(unhandled, predicates);
    }
    handledPredicates = handled.toArray(new Predicate[0]);
    return unhandled.toArray(new Predicate[0]);
}

@Override
public Predicate[] pushedPredicates() { return handledPredicates; }

// Extract single field name from a 1-element NamedReference array
private static String fieldName(NamedReference[] refs) {
    return refs.length == 1 ? refs[0].fieldNames()[0] : "";
}
```

Note: unlike the V1 implementation (which returned all filters as unhandled and relied
on Spark to post-filter), V2 allows returning handled predicates. For indexed reads
(BAI/tabix/FAI), the index-guided range is exact — so range predicates on `start`/`pos`
**can** be returned as handled when an index was found during `build()`. However, since
we don't know at `pushPredicates()` time whether an index exists (that's resolved in
`BamScan.planInputPartitions()`), keep returning them as unhandled for safety. The
`pushedPredicates()` return value only affects `EXPLAIN` output, not correctness.

**VCF — `VcfScanBuilder.java`**

Identical pattern; equality on `chrom`, range on `pos` (IntegerType).

**BED — `BedScanBuilder.java`**

Equality on `chrom`, range on `chromStart`/`chromEnd` (LongType, 0-based).
BED currently pushes only `chromStart` for the lower bound. V2 predicate gives the
opportunity to also push `chromEnd` upper-bound predicates to tabix — add that.

**FASTA — `FastaScanBuilder.java`**

Equality on `name` only. No range fields. Implementation is a single-predicate extract
with no second pass.

### Tests

Each `*ScanBuilderTest` already has `pushFilters()` unit tests. For each:
- Rename test class or add a parallel `pushPredicatesTest()` section
- Construct `Predicate` objects using `org.apache.spark.sql.connector.expressions.filter`
  factories instead of `new EqualTo(...)`
- Verify pushed field values on the resulting `*Scan` (same assertions, different setup)
- Add a test for `And(EqualTo(chrom), GreaterThanOrEqual(pos))` to confirm nested `And`
  unwrapping works (BAM and VCF)

---

## Feature 2: `SupportsReportStatistics`

### Why

Without statistics, Spark's query optimizer is blind to table sizes. When joining a
BAM against a VCF, Spark picks between broadcast join (fast, requires small table in
memory) and sort-merge join (scalable, requires shuffle) based on `spark.sql.autoBroadcastJoinThreshold`. If both tables report `Unknown`, Spark applies its
default heuristic and may choose wrong. Even reporting only `sizeInBytes` (from the
file size on disk) is enough to get the join strategy right.

### What changes

`SupportsReportStatistics` is on the `Scan` class (not `ScanBuilder`). It has one method:

```java
// org.apache.spark.sql.connector.read
interface SupportsReportStatistics {
    Statistics estimateStatistics();
}
```

`Statistics` is:
```java
interface Statistics {
    OptionalLong sizeInBytes();
    OptionalLong numRows();
}
```

### Implementation per format

**BAM/CRAM — `BamScan.java`**

Add `implements SupportsReportStatistics` to `BamScan`.

```java
@Override
public Statistics estimateStatistics() {
    try {
        Configuration conf = SparkSession.builder().getOrCreate()
            .sessionState().newHadoopConf();
        long totalBytes = 0;
        // Reuse the same file-resolution logic already in planInputPartitions()
        // (or extract a shared helper resolveFiles(options, conf) -> List<FileStatus>)
        for (FileStatus fs : resolveFiles(options, conf)) {
            totalBytes += fs.getLen();
        }
        final long size = totalBytes;
        return new Statistics() {
            public OptionalLong sizeInBytes() { return OptionalLong.of(size); }
            public OptionalLong numRows()     { return OptionalLong.empty(); }
        };
    } catch (IOException e) {
        return new Statistics() {
            public OptionalLong sizeInBytes() { return OptionalLong.empty(); }
            public OptionalLong numRows()     { return OptionalLong.empty(); }
        };
    }
}
```

The key refactor prerequisite: extract `resolveFiles()` as a package-private static
helper out of `planInputPartitions()` in `BamScan`. That method currently inlines the
glob/directory expansion. This same helper can be reused by `estimateStatistics()` without
re-planning partitions.

**VCF — `VcfScan.java`**, **BED — `BedScan.java`**, **FASTA — `FastaScan.java`**, **FASTQ — `FastqScan.java`**

Same pattern. All five `*Scan` classes inline a similar file-resolution block in
`planInputPartitions()`. Extract `resolveFiles()` from each and use it in both methods.

**FASTA enhanced (when FAI exists)**

FASTA is the one format where `numRows` can be known cheaply:
- If an FAI index was resolved, it was already read into a list of contig entries
- `numRows = number of contigs in the FAI` (or 1 for a full-file partition)
- Pass this value from `FastaScan`'s constructor (it already receives `pushedName`,
  so just also receive `faiEntryCount` from the scan builder)

```java
// In FastaScan:
private final int faiEntryCount; // -1 = unknown (no FAI)

@Override
public Statistics estimateStatistics() {
    OptionalLong rows = faiEntryCount >= 0
        ? OptionalLong.of(faiEntryCount)
        : OptionalLong.empty();
    // ... file size as above
    return new Statistics() {
        public OptionalLong sizeInBytes() { return OptionalLong.of(size); }
        public OptionalLong numRows()     { return rows; }
    };
}
```

### Tests

Statistics are best tested as integration tests in `*DataSourceTest`. After reading a
file, call:
```java
Dataset<Row> df = spark.read().format("bam").option("path", path).load();
// Access the physical plan's statistics
long sizeBytes = df.queryExecution().optimizedPlan()
    .stats().sizeInBytes().longValue();
assertTrue(sizeBytes > 0);
assertTrue(sizeBytes <= new File(path).length() * 2); // within 2x of file size
```

For FASTA with FAI, also assert `numRows == expectedContigCount`.

---

## Feature 3: `SupportsReportOrdering`

### Why

BAM files are coordinate-sorted by the SAM spec (when `SO:coordinate` is in the header).
Within each per-reference VFO partition, reads are sorted by `start` position. If Spark
knows this, it can skip sort stages — for example when doing a merge join between BAM
and a VCF sorted by `[chrom, pos]`, or when `ORDER BY referenceName, start` is the final
query operation.

`SupportsReportOrdering` reports **local** (within-partition) ordering, not global ordering
across the whole dataset. This is still valuable: Spark uses it to avoid sorting before
merge joins when both sides report compatible local ordering.

### What changes

`SupportsReportOrdering` is on the `Scan` class:

```java
interface SupportsReportOrdering {
    SortOrder[] outputOrdering();
}
```

`SortOrder` is `org.apache.spark.sql.connector.expressions.SortOrder`, constructed via
`SortOrder.apply(expression, direction, nullOrdering)`.

### Format-specific applicability

| Format | Can report ordering? | Condition | Order |
|---|---|---|---|
| BAM | Yes | `SO:coordinate` in SAM header AND BAI present (VFO partitions) | `referenceName ASC NULLS LAST, start ASC NULLS LAST` |
| CRAM | Yes | Same condition | Same |
| VCF | Yes | Tabix index present | `chrom ASC NULLS LAST, pos ASC NULLS LAST` |
| BED | Yes | Tabix index present | `chrom ASC NULLS LAST, chromStart ASC NULLS LAST` |
| FASTA | No | Contigs appear in FAI order but order is arbitrary | `[]` |
| FASTQ | No | No ordering guarantee | `[]` |

**Important caveat for BAM:** the SAM header `SO` tag must be checked. If `SO` is
`queryname` or `unsorted`, returning coordinate ordering would be wrong and could silently
corrupt join results. This check must happen in `BamScan` during `planInputPartitions()`
when the SAM header is already opened.

### Implementation per format

**BAM/CRAM — `BamScan.java`**

1. Add `implements SupportsReportOrdering` to `BamScan`.
2. Add a field `private boolean isCoordinateSorted = false`.
3. In `planInputPartitions()`, after opening the SAM header (which already happens to read
   reference sequences), check:
   ```java
   isCoordinateSorted = header.getSortOrder() == SAMFileHeader.SortOrder.coordinate
       && indexPath != null; // only meaningful when VFO partitions are planned
   ```
4. Implement:
   ```java
   @Override
   public SortOrder[] outputOrdering() {
       if (!isCoordinateSorted) return new SortOrder[0];
       return new SortOrder[] {
           SortOrder.apply(
               FieldReference.apply("referenceName"),
               SortDirection.ASCENDING,
               NullOrdering.NULLS_LAST),
           SortOrder.apply(
               FieldReference.apply("start"),
               SortDirection.ASCENDING,
               NullOrdering.NULLS_LAST)
       };
   }
   ```

**Ordering lazy initialization problem:** `outputOrdering()` may be called before or after
`planInputPartitions()`. The safest fix is to separate the header-reading logic into a
lazily initialized helper `ensureHeaderRead()` that both methods can call. Alternatively,
store the sort-order state in a field set during `readSchema()` (which is called first in
Spark's planning flow) by opening and immediately closing the SAM header there.

**VCF — `VcfScan.java`**

Add `implements SupportsReportOrdering`. Add a `boolean hasTabixIndex` field populated in
`planInputPartitions()` (the tabix resolution already happens there). If tabix was found:
```java
return new SortOrder[] {
    SortOrder.apply(FieldReference.apply("chrom"), ASCENDING, NULLS_LAST),
    SortOrder.apply(FieldReference.apply("pos"),   ASCENDING, NULLS_LAST)
};
```

**BED — `BedScan.java`**

Same as VCF. Use `chrom ASC, chromStart ASC`.

### Tests

Add tests in `*DataSourceTest` that verify ordering is reported when an index is present
and not reported when it is absent:

```java
// BAM with BAI → should report coordinate ordering
Dataset<Row> df = spark.read().format("bam").option("path", bamWithBai).load();
// Access Spark's physical plan ordering
List<SortOrder> ordering = ScalaUtils.toJava(
    df.queryExecution().sparkPlan().outputOrdering());
assertFalse(ordering.isEmpty());

// SAM (no index) → no ordering
Dataset<Row> dfSam = spark.read().format("bam").option("path", samFile).load();
assertTrue(ScalaUtils.toJava(dfSam.queryExecution().sparkPlan().outputOrdering()).isEmpty());
```

---

## Feature 4: `SupportsPushDownLimit`

### Why

Without limit pushdown, `SELECT * FROM fastq LIMIT 100` plans all partitions, opens
every file split on every executor, and reads the first 100 records — then Spark's driver
discards everything else. For FASTQ (no index, no filter) this means every file is
opened. With limit pushdown, the connector can stop after the first partition fills the
limit.

`SupportsPushDownLimit` is on the `ScanBuilder`:

```java
interface SupportsPushDownLimit {
    boolean pushLimit(int limit);
    // Returns true if connector guarantees returning <= limit rows (Spark skips local limit).
    // Returns false if connector returns approximately limit rows (Spark still limits).
}
```

Always return `false` here. Spark will still apply a local limit, which is correct.
Returning `false` means we use the limit as a hint, not a guarantee — safe when a
partition might have slightly more rows than the limit.

### Implementation per format

The limit value flows: `ScanBuilder.pushLimit()` → stored field → `Scan` constructor →
`InputPartition` → `PartitionReader.next()`.

**FASTQ — highest value, implement first**

`FastqScanBuilder.java`:
1. Add `implements SupportsPushDownLimit`
2. Add field `private int pushedLimit = Integer.MAX_VALUE`
3. Implement `pushLimit(int limit)`: set `pushedLimit = limit; return false;`
4. Pass `pushedLimit` to `FastqScan` constructor

`FastqScan.java`:
1. Accept `pushedLimit` in constructor
2. In `planInputPartitions()`: if `pushedLimit < Integer.MAX_VALUE`, plan only one
   partition (the first split) rather than all splits. This avoids opening files on
   all executors.

`FastqInputPartition.java`:
1. Add a `rowLimit` field (default `Integer.MAX_VALUE`)

`FastqPartitionReader.java`:
1. Track `long rowsRead = 0`
2. In `next()`: `if (rowsRead >= partition.rowLimit()) return false;`
3. Increment `rowsRead` in `get()` (or at the top of `next()`)

**BAM/CRAM — `BamScanBuilder.java`, `BamScan.java`, `BamInputPartition.java`, `BamPartitionReader.java`**

Same flow as FASTQ. Additional optimization in `BamScan.planInputPartitions()`:
if `pushedLimit < Integer.MAX_VALUE` AND no region was pushed down, plan only
the first one or two partitions instead of one per reference sequence.

**VCF — `VcfScanBuilder.java`, `VcfScan.java`, `VcfInputPartition.java`, `VcfPartitionReader.java`**

Same flow.

**BED — `BedScanBuilder.java`, `BedScan.java`, `BedInputPartition.java`, `BedPartitionReader.java`**

Same flow.

**FASTA — `FastaScanBuilder.java`, `FastaScan.java`, `FastaInputPartition.java`, `FastaPartitionReader.java`**

Same flow. In `FastaScan.planInputPartitions()`, if `pushedLimit = 1` and we would plan
N contig partitions, plan only 1 instead.

### Tests

Add a test to each `*DataSourceTest`:
```java
// Read with LIMIT — should complete quickly and return <= limit rows
Dataset<Row> df = spark.read().format("fastq")
    .option("path", largeFastqFile)
    .load()
    .limit(10);
assertEquals(10, df.count());
// Bonus: assert that only 1 partition was planned (via df.rdd().getNumPartitions())
```

For BAM/VCF/BED: assert that a `LIMIT 5` with a region filter still returns correct rows.

---

## Implementation Order

Do these in order — each step is independently mergeable and leaves the codebase in a
correct state.

### Phase 1 — Foundation refactors (no new API, unblocks everything else)

These refactors are prerequisites with no user-visible behavior change.

1. **Extract `resolveFiles()` helper** from `planInputPartitions()` in each `*Scan` class.
   Returns `List<FileStatus>`. Used by both `estimateStatistics()` and `planInputPartitions()`.
   One commit per format, or batch all five.

2. **Extract `ensureHeaderRead()` in `BamScan`** — lazily opens the SAM header and sets
   `isCoordinateSorted`. Needed for Feature 3 to work regardless of call order.

### Phase 2 — Statistics (Feature 2, all formats)

Low risk, high value. No interface incompatibility with existing V1 pushdown.

Order: FASTQ → FASTA (with numRows) → BAM → VCF → BED

Each format is one commit:
- Add `implements SupportsReportStatistics` to `*Scan`
- Add `estimateStatistics()` using `resolveFiles()` from Phase 1
- Add integration test

### Phase 3 — Limit pushdown (Feature 4, all formats)

Medium complexity. Threads a new field through four classes per format but is
self-contained per format.

Order: FASTQ (biggest win) → BAM → VCF → BED → FASTA

Each format is one commit touching `*ScanBuilder`, `*Scan`, `*InputPartition`, `*PartitionReader`.

### Phase 4 — Ordering (Feature 3, BAM/VCF/BED)

Medium risk — incorrect ordering declarations corrupt sort-sensitive queries. Implement
with conservative conditions (only when index is confirmed present).

Order: VCF → BED → BAM/CRAM

Each format is one commit. BAM last because it requires the header `SO` tag check.

### Phase 5 — V2 predicate pushdown (Feature 1, BAM/VCF/BED/FASTA)

Low behavior risk (same logic, new types) but removes V1 API. Do last so all other
features are already tested with V1 pushdown.

Order: FASTA (simplest, equality-only) → VCF → BED → BAM/CRAM

Each format is one commit replacing the `ScanBuilder` pushdown impl and updating its unit test.

---

## Cross-cutting concerns

### `SupportsPushDownV2Filters` vs. `SupportsPushDownFilters` coexistence

Spark checks for `SupportsPushDownV2Filters` first and skips V1 if it's present.
When migrating a format in Phase 5, remove the V1 interface and its imports in the same
commit. Do not implement both — it's redundant and confusing.

### Ordering declarations and correctness

**Never** return a non-empty `outputOrdering()` unless the data within each partition is
guaranteed sorted in that order. A wrong declaration causes silent correctness bugs in
sort-merge joins and ordered aggregations. For BAM, check `header.getSortOrder()` at
runtime, not the file extension or a user option.

### Statistics accuracy

`sizeInBytes` from `FileStatus.getLen()` reflects the compressed size on disk, not the
uncompressed row count. This is fine — Spark uses it for broadcast-join threshold
comparisons, not for memory allocation. Do not attempt to decompress or scan to get
an accurate row count (except for FASTA's FAI path, which is free).

### `pushLimit` and region-filtered reads

When both a genomic region filter AND a limit are pushed, apply the limit on top of the
region result — don't skip the region filter to satisfy the limit faster. The region filter
is the expensive optimization (avoiding full-file reads); the limit is secondary.

### Spark version compatibility

`SupportsPushDownV2Filters` was added in Spark 3.3. All test targets (Spark 4.0.2,
4.1.1, Databricks 17.3-LTS) are ≥ 3.3, so there is no compatibility concern.
`SupportsPushDownLimit` was added in Spark 3.0. `SupportsReportOrdering` and
`SupportsReportStatistics` have been stable since Spark 3.0.

---

## Files touched summary

| File | Features |
|---|---|
| `bam/BamScanBuilder.java` | 1 (V2 filters), 4 (limit) |
| `bam/BamScan.java` | 2 (statistics), 3 (ordering), 4 (limit) |
| `bam/BamInputPartition.java` | 4 (limit — add `rowLimit` field) |
| `bam/BamPartitionReader.java` | 4 (limit — check `rowsRead`) |
| `vcf/VcfScanBuilder.java` | 1, 4 |
| `vcf/VcfScan.java` | 2, 3, 4 |
| `vcf/VcfInputPartition.java` | 4 |
| `vcf/VcfPartitionReader.java` | 4 |
| `bed/BedScanBuilder.java` | 1, 4 |
| `bed/BedScan.java` | 2, 3, 4 |
| `bed/BedInputPartition.java` | 4 |
| `bed/BedPartitionReader.java` | 4 |
| `fasta/FastaScanBuilder.java` | 1, 4 |
| `fasta/FastaScan.java` | 2, 4 |
| `fasta/FastaInputPartition.java` | 4 |
| `fasta/FastaPartitionReader.java` | 4 |
| `fastq/FastqScanBuilder.java` | 4 |
| `fastq/FastqScan.java` | 2, 4 |
| `fastq/FastqInputPartition.java` | 4 |
| `fastq/FastqPartitionReader.java` | 4 |
| Test files (each format's `*DataSourceTest` and `*ScanBuilderTest`) | New test cases per feature |
