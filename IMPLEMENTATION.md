# Implementation Checklist

## Root build configuration

- [x] `pom.xml` — parent POM, `packaging=pom`
  - [x] `<modules>`: `core` (scala to be added when that module starts)
  - [x] `<properties>`: `spark.version=4.0.2`, `scala.version=2.13.16`, `scala.binary.version=2.13`, `java.version=17`, `htsjdk.version=4.3.0`
  - [x] `<dependencyManagement>`: `spark-sql_2.13` (provided), `hadoop-client` (provided), `htsjdk` (compile), `junit-jupiter` (test)
  - [x] `<pluginManagement>`: `maven-compiler-plugin` (Java 17), `maven-shade-plugin 3.5.2`, `maven-surefire-plugin 3.2.5` with `--add-opens` for Spark

---

## Core module (`core/`)

### Build configuration

- [x] `core/pom.xml` — groupId `com.litebfx`, artifactId `lite-bfx-spark`, parent reference
  - [x] `spark-sql_2.13:${spark.version}` as `provided`
  - [x] `hadoop-client:3.3.6` as `provided`
  - [x] `htsjdk:4.3.0` as `compile`
  - [x] `junit-jupiter:5.10.2` as `test`
  - [x] `maven-shade-plugin` relocating `htsjdk.**` → `com.litebfx.shaded.htsjdk.**`
  - [x] `maven-shade-plugin` relocating `com.google.common.**` → `com.litebfx.shaded.guava.**`

### Source files

- [x] `core/src/main/java/com/litebfx/BamSchema.java`
  - [x] Static `StructType SCHEMA` with all 12 fields (readName, flags, referenceName, start, mappingQuality, cigar, mateReferenceName, mateStart, insertSize, sequence, baseQualities, attributes)

- [x] `core/src/main/java/com/litebfx/SerializableConfiguration.java` _(not in original plan — added during implementation)_
  - [x] Wraps `Configuration` using `Writable.write/readFields` for Java serialization
  - [x] Used by `BamInputPartition` instead of `SerializableWritable` (avoids Hadoop mapred dependency)

- [x] `core/src/main/java/com/litebfx/HadoopSeekableStream.java`
  - [x] Implements `htsjdk.samtools.seekablestream.SeekableStream`
  - [x] Constructor accepts `FSDataInputStream` + file length + source string
  - [x] `read(byte[], int, int)` delegates to stream
  - [x] `seek(long)` delegates to `FSDataInputStream.seek()`
  - [x] `position()` delegates to `FSDataInputStream.getPos()`
  - [x] `length()` returns file length
  - [x] `eof()` returns `pos >= length`
  - [x] `close()` closes stream

- [x] `core/src/main/java/com/litebfx/BamInputPartition.java`
  - [x] Implements `InputPartition` and `Serializable`
  - [x] Fields: `path` (String), `startVirtualOffset` (long), `endVirtualOffset` (long), `hadoopConf` (`SerializableConfiguration`)

- [x] `core/src/main/java/com/litebfx/BamPartitionReader.java`
  - [x] Implements `PartitionReader<InternalRow>`
  - [x] Opens `FileSystem` using partition's serialized Hadoop config
  - [x] Opens BAM/SAM via `SamReaderFactory` (LENIENT) + `HadoopSeekableStream`
  - [x] `next()` advances iterator; lazy-opens on first call
  - [x] `get()` converts current `SAMRecord` to `InternalRow`:
    - [x] All 11 scalar fields mapped to correct Spark types (strings as `UTF8String`, nulls preserved)
    - [x] `mateReferenceName` via index lookup (htsjdk 4.x removed `getMateContig()`)
    - [x] `attributes` map built from SAM optional tags (null when `includeAttributes=false`)
  - [x] `close()` closes iterator, `SamReader`, and `FSDataInputStream`
  - [ ] VFO seeking for `startVirtualOffset > 0` _(deferred: htsjdk 4.x `SAMFileSource.getFilePointer()` returns `SAMFileSpan` not `long`; will be resolved in `BamScan`)_
  - [ ] End-boundary enforcement via VFO _(deferred: htsjdk 4.x type resolution pending)_

- [x] `core/src/main/java/com/litebfx/BamDataSource.java`
  - [x] Implements `TableProvider` and `DataSourceRegister`
  - [x] `shortName()` returns `"bam"`
  - [x] `inferSchema()` returns `BamSchema.SCHEMA`
  - [x] `getTable()` returns new `BamTable`

- [x] `core/src/main/java/com/litebfx/BamTable.java`
  - [x] Implements `Table` and `SupportsRead`
  - [x] `name()` returns BAM path from options
  - [x] `schema()` returns `BamSchema.SCHEMA`
  - [x] `capabilities()` returns `BATCH_READ`
  - [x] `newScanBuilder()` returns new `BamScanBuilder`

- [x] `core/src/main/java/com/litebfx/BamScanBuilder.java`
  - [x] Implements `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns`
  - [x] `pushFilters()` — extracts `referenceName` equality and `start` range; returns ALL filters as unhandled (Spark post-filters for correctness; BAI optimization is transparent)
  - [x] `pushedFilters()` — returns empty (all filters are post-filtered by Spark)
  - [x] `pruneColumns()` — records required schema; sets `includeAttributes` flag
  - [x] `build()` returns `BamScan` with pushed reference, region bounds, required schema, and options

- [x] `core/src/main/java/com/litebfx/BamScan.java`
  - [x] Implements `Scan` and `Batch`
  - [x] `readSchema()` returns required schema (pruned or full)
  - [x] `toBatch()` returns `this`
  - [x] `planInputPartitions()`:
    - [x] Resolves list of BAM/SAM files from `path` option (single file, directory, or glob via `FileSystem.globStatus()`)
    - [x] BAI resolution per file: `indexPath` option → `indexDir/<name>.bai` → co-located `<path>.bai` → none
    - [x] For BAM + BAI + pushed region: sets `querySequence` on partition → `BamPartitionReader` calls `samReader.query()` (uses htsjdk BAI internally)
    - [x] For BAM + BAI, no region: single partition, `samReader.iterator()` (full scan)
    - [x] For BAM, no BAI: single partition `[0, Long.MAX_VALUE]`
    - [x] For SAM: single partition `[0, Long.MAX_VALUE]` (BAI resolution skipped, `useIndex` flag respected)
    - [ ] `numPartitions` cap per file _(deferred: requires VFO-based splitting)_
    - [x] Captures `SparkSession.hadoopConfiguration()` into each `BamInputPartition`
    - [ ] VFO chunk splitting via `BAMFileSpan` _(deferred: htsjdk 4.x type resolution pending)_
  - [x] `createReaderFactory()` returns new `BamPartitionReaderFactory`

- [x] `core/src/main/java/com/litebfx/BamPartitionReaderFactory.java`
  - [x] Implements `PartitionReaderFactory`
  - [x] `createReader()` returns new `BamPartitionReader` for given partition

### Service registration

- [x] `core/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
  - [x] Contains single line: `com.litebfx.BamDataSource`

### Tests

- [x] `core/src/test/resources/range.bam` — real C. elegans Illumina paired-end BAM (htslib test suite, 112 records, 14 KB)
- [x] `core/src/test/resources/range.bam.bai` — BAI index for range.bam
- [x] `core/src/test/java/com/litebfx/TestBamGenerator.java` — generates synthetic BAM + BAI + SAM programmatically (10 records on chr1, deterministic)

- [x] `core/src/test/java/com/litebfx/BamPartitionReaderTest.java` _(unit tests, no SparkSession)_
  - [x] Schema: 12 fields, correct names
  - [x] BAM: full-file read count = 10 (synthetic)
  - [x] BAM: first record scalar fields (readName, flags, referenceName, start, MQ, cigar, mateStart, insertSize, sequence, baseQualities)
  - [x] BAM: start positions monotonically increasing (100, 200, …, 1000)
  - [x] BAM: read names match `read1`…`read10`
  - [x] BAM: attributes map contains `NM=0`
  - [x] BAM: attributes null when `includeAttributes=false`
  - [x] SAM: count matches BAM count
  - [x] SAM: first record fields correct
  - [x] Non-zero `startVirtualOffset` throws `UnsupportedOperationException` _(to be lifted when VFO seeking is implemented in `BamScan`)_

- [x] `core/src/test/java/com/litebfx/RangeBamTest.java` _(real-world data, no SparkSession)_
  - [x] Total count = 112 (matches `samtools view -c`)
  - [x] Per-chromosome counts: I=18, II=34, III=41, IV=19 (matches samtools)
  - [x] First record: readName, flags=145, CHROMOSOME_I:914, MQ=23, CIGAR=78M1D22M
  - [x] First record: mate on CHROMOSOME_V:1104758 (cross-chromosome pair)
  - [x] First record: sequence and base quality strings match samtools output byte-for-byte
  - [x] First record: attributes NM=3, RG=1
  - [x] CIGAR variety: deletion (D), soft-clip (S), 100M all present
  - [x] All 112 reads have non-null readName and referenceName

- [x] `core/src/test/java/com/litebfx/BamDataSourceTest.java`
  - [x] BAM read via `spark.read.format("bam").load(path)`: schema matches `BamSchema.SCHEMA`
  - [x] BAM read: `df.count()` = 112
  - [x] BAM predicate pushdown: `referenceName = 'CHROMOSOME_I'` → count = 18
  - [x] BAM predicate pushdown: `referenceName = 'CHROMOSOME_I' AND start >= 1000 AND start <= 2000` → count = 14
  - [x] BAM no-index fallback: `useIndex=false` returns count = 112
  - [x] BAM explicit `indexPath` option works
  - [x] SAM read: same schema, count = 10 (TestBamGenerator SAM)
  - [x] Column pruning: `select("readName", "start")` returns 2-column schema
  - [x] Column pruning: select without `attributes` succeeds (no NPE from skipped map)

---

---

## CRAM module (`core/src/main/java/com/litebfx/cram/`)

- [ ] `core/src/main/java/com/litebfx/cram/CramDataSource.java`
  - [ ] Implements `TableProvider` and `DataSourceRegister`
  - [ ] `shortName()` returns `"cram"`
  - [ ] `getTable()` returns `BamTable` constructed with `isCram=true` flag and `referenceFile` option forwarded

_All remaining CRAM logic lives in existing `Bam*` classes; `isCram=true` gates CRAI resolution and `SamReaderFactory.referenceSource()` configuration._

- [ ] `core/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
  - [ ] Add line: `com.litebfx.cram.CramDataSource`

### CRAM changes to existing classes

- [ ] `BamScan.planInputPartitions()` — when `isCram=true`:
  - [ ] CRAI resolution: `indexPath` option → `indexDir/<name>.cram.crai` → co-located `<path>.cram.crai` → single partition
  - [ ] Open `CRAIIndex` from resolved path; iterate `CRAIEntry` list to build `BamInputPartition` list with VFO ranges
  - [ ] Pass `referenceFile` + `referenceMode` into each partition's serialized options
- [ ] `BamPartitionReader` — when `isCram=true`:
  - [ ] Configure `SamReaderFactory.referenceSource()` using `referenceFile` option before opening reader
  - [ ] `referenceMode=none` → `SamReaderFactory.referenceSource(ReferenceSource.NULL_REFERENCE_SOURCE)`

### Tests

- [ ] `core/src/test/java/com/litebfx/cram/CramDataSourceTest.java`
  - [ ] CRAM read via `spark.read.format("cram").option("referenceFile", ...).load(path)`: schema matches `BamSchema.SCHEMA`
  - [ ] `df.count()` equals `samtools view -c test.cram`
  - [ ] Region query returns expected count
  - [ ] Missing CRAI falls back to single partition and still returns correct count

---

## FASTQ module (`core/src/main/java/com/litebfx/fastq/`)

### Build

- [ ] No new Maven dependencies — `htsjdk` already provides `FastqReader`

### Source files

- [ ] `FastqSchema.java`
  - [ ] Static `StructType SCHEMA` with 4 fields: `readName` (String), `sequence` (String), `baseQualities` (String), `description` (String, nullable)

- [ ] `FastqInputPartition.java`
  - [ ] Fields: `path` (String), `startByte` (long), `endByte` (long), `hadoopConf` (SerializableConfiguration)
  - [ ] `startByte=0, endByte=Long.MAX_VALUE` for single-partition (gzipped or no-split)

- [ ] `FastqPartitionReader.java`
  - [ ] Opens `FSDataInputStream`, seeks to `startByte`
  - [ ] Advances to next `@` line boundary before starting iteration (boundary scan)
  - [ ] Iterates via `htsjdk.samtools.fastq.FastqReader`
  - [ ] Stops when stream position exceeds `endByte` (end-of-partition boundary)
  - [ ] `get()` maps `FastqRecord` → `InternalRow`: `readName` (strip leading `@`), `sequence`, `baseQualities`, `description`

- [ ] `FastqScan.java`
  - [ ] `planInputPartitions()`:
    - [ ] Detects gzip from extension (`.fastq.gz`, `.fq.gz`) → single partition `[0, MAX_VALUE]`
    - [ ] Uncompressed: get file size via `FileSystem.getFileStatus(path).getLen()`; divide into `min(numPartitions, fileSize/MIN_SPLIT_BYTES)` byte-range partitions
    - [ ] `MIN_SPLIT_BYTES` constant: 64 MB

- [ ] `FastqPartitionReaderFactory.java`, `FastqTable.java`, `FastqDataSource.java` — standard boilerplate (same pattern as BAM equivalents)

- [ ] `DataSourceRegister` entry: `com.litebfx.fastq.FastqDataSource`

### Tests

- [ ] `core/src/test/java/com/litebfx/fastq/FastqDataSourceTest.java`
  - [ ] Schema: 4 fields, correct names and types
  - [ ] Count matches `wc -l / 4` for uncompressed FASTQ
  - [ ] `readName`, `sequence`, `baseQualities` of first record match raw file
  - [ ] Gzipped FASTQ: single partition, correct count
  - [ ] `description` field is null when FASTQ header has no description

---

## VCF module (`core/src/main/java/com/litebfx/vcf/`)

### Build

- [ ] No new Maven dependencies — `htsjdk` includes Tribble VCF reader and tabix support

### Source files

- [ ] `VcfSchema.java`
  - [ ] Static `StructType SCHEMA`: `chrom` (String), `pos` (Int), `id` (String nullable), `ref` (String), `alt` (String nullable), `qual` (Double nullable), `filter` (String nullable), `info` (MapType(String,String)), `format` (String nullable), `genotypes` (MapType(String,String) nullable)

- [ ] `VcfInputPartition.java`
  - [ ] Fields: `path` (String), `startVirtualOffset` (long), `endVirtualOffset` (long), `queryChrom` (String nullable), `queryStart` (int), `queryEnd` (int), `hadoopConf` (SerializableConfiguration)

- [ ] `VcfPartitionReader.java`
  - [ ] Opens `VCFFileReader` over `HadoopSeekableStream`
  - [ ] If `queryChrom != null`: calls `reader.query(queryChrom, queryStart, queryEnd)`; else calls `reader.iterator()`
  - [ ] `get()` maps `VariantContext` → `InternalRow`:
    - [ ] `chrom` = `vc.getContig()`, `pos` = `vc.getStart()`, `id` = `vc.getID()` (null if `.`)
    - [ ] `ref` = `vc.getReference().getDisplayString()`
    - [ ] `alt` = comma-join of `vc.getAlternateAlleles()` (null if empty/missing)
    - [ ] `qual` = `vc.hasLog10PError() ? vc.getPhredScaledQual() : null`
    - [ ] `filter` = join of `vc.getFilters()` (null if `.`)
    - [ ] `info` = map from `vc.getAttributes()` (values `.toString()`; flag attributes stored as `"true"`)
    - [ ] `format` = colon-join of `vc.getGenotypes().get(0).getExtendedAttributes()` keys (null when no samples)
    - [ ] `genotypes` = map of sampleName → colon-join of format values (null when no samples)

- [ ] `VcfScanBuilder.java`
  - [ ] Implements `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns`
  - [ ] Pushable filters: `chrom = 'X'` (EqualTo), `pos >= A` (GreaterThanOrEqual), `pos <= B` (LessThanOrEqual)
  - [ ] All pushed filters returned as unhandled (Spark post-filters for safety)

- [ ] `VcfScan.java`
  - [ ] `planInputPartitions()`:
    - [ ] Tabix index resolution: `indexPath` → co-located `.tbi` → co-located `.csi` → single partition
    - [ ] With tabix + pushed region: `TabixIndex.getBlocks(chrom, start, end)` → one or more `VcfInputPartition` with VFO ranges and query coordinates
    - [ ] No index or no pushed region: single partition `[0, MAX_VALUE]`, `queryChrom=null`

- [ ] `VcfPartitionReaderFactory.java`, `VcfTable.java`, `VcfDataSource.java` — standard boilerplate

- [ ] `DataSourceRegister` entry: `com.litebfx.vcf.VcfDataSource`

### Tests

- [ ] `core/src/test/java/com/litebfx/vcf/VcfDataSourceTest.java`
  - [ ] Schema: 10 fields, correct names and types
  - [ ] Count matches `bcftools view -H test.vcf.gz | wc -l`
  - [ ] Region query `chrom + pos range` returns expected count
  - [ ] `info` map contains expected key-value pairs
  - [ ] `genotypes` map keyed by sample name
  - [ ] No-tabix fallback: single partition, correct count
  - [ ] BCF file reads with `format("vcf")`
  - [ ] Uncompressed `.vcf` reads correctly

---

## FASTA module (`core/src/main/java/com/litebfx/fasta/`)

### Build

- [ ] No new Maven dependencies — `htsjdk` provides `IndexedFastaSequenceFile` and `ReferenceSequenceFile`

### Source files

- [ ] `FastaSchema.java`
  - [ ] Static `StructType SCHEMA`: `name` (String), `sequence` (String), `length` (Long)

- [ ] `FastaInputPartition.java`
  - [ ] Fields: `path` (String), `contigName` (String nullable — null means full-file single partition), `hadoopConf` (SerializableConfiguration)

- [ ] `FastaPartitionReader.java`
  - [ ] If `contigName != null`: opens `IndexedFastaSequenceFile`, calls `getSequence(contigName)`
  - [ ] If `contigName == null`: opens `ReferenceSequenceFile`, iterates all sequences
  - [ ] `get()` maps `ReferenceSequence` → `InternalRow`: `name`, `sequence.getBases()` as String, `length`

- [ ] `FastaScan.java`
  - [ ] `planInputPartitions()`:
    - [ ] FAI resolution: `indexPath` → co-located `<path>.fai` → single partition
    - [ ] With FAI: read `.fai` to get contig list; one `FastaInputPartition` per contig (respects `numPartitions` cap — merge small contigs if needed)
    - [ ] No FAI: single partition with `contigName=null`

- [ ] `FastaPartitionReaderFactory.java`, `FastaTable.java`, `FastaDataSource.java` — standard boilerplate

- [ ] `DataSourceRegister` entry: `com.litebfx.fasta.FastaDataSource`

### Tests

- [ ] `core/src/test/java/com/litebfx/fasta/FastaDataSourceTest.java`
  - [ ] Schema: 3 fields
  - [ ] Contig count matches `grep -c '^>' ref.fa`
  - [ ] Each row's `length` matches `.fai` length column
  - [ ] `sequence` for a known contig matches `samtools faidx ref.fa contigName`
  - [ ] No-FAI fallback: single partition, all contigs present

---

## BED module (`core/src/main/java/com/litebfx/bed/`)

### Build

- [ ] No new Maven dependencies — `htsjdk` Tribble includes `BEDCodec` and `AbstractFeatureReader`

### Source files

- [ ] `BedSchema.java`
  - [ ] Static `StructType SCHEMA` with 12 nullable fields (see schema table in PLAN.md)
  - [ ] All fields beyond `chromEnd` are nullable

- [ ] `BedInputPartition.java`
  - [ ] Fields: `path` (String), `startVirtualOffset` (long), `endVirtualOffset` (long), `queryChrom` (String nullable), `queryStart` (long), `queryEnd` (long), `hadoopConf` (SerializableConfiguration)

- [ ] `BedPartitionReader.java`
  - [ ] Opens `AbstractFeatureReader<BEDFeature, LineIterator>` via `AbstractFeatureReader.getFeatureReader(path, new BEDCodec(), index)`
  - [ ] If `queryChrom != null`: calls `reader.query(queryChrom, (int)queryStart, (int)queryEnd)`; else `reader.iterator()`
  - [ ] `get()` maps `BEDFeature` → `InternalRow`:
    - [ ] `chrom`, `chromStart` (0-based), `chromEnd`, `name` (null if `.`), `score`, `strand`
    - [ ] `thickStart`, `thickEnd`, `itemRgb`, `blockCount`, `blockSizes`, `blockStarts` (null when not present)
  - [ ] Column count detected on first record; sets `numColumns` field used to null-fill missing columns

- [ ] `BedScanBuilder.java`
  - [ ] Pushable filters: `chrom = 'X'` (EqualTo), `chromStart >= A` (GreaterThanOrEqual), `chromEnd <= B` (LessThanOrEqual)
  - [ ] All pushed filters returned as unhandled (Spark post-filters for safety)

- [ ] `BedScan.java`
  - [ ] `planInputPartitions()`:
    - [ ] Tabix resolution: `indexPath` → co-located `.tbi` → co-located `.csi` → single partition
    - [ ] With tabix + pushed region: `TabixIndex.getBlocks(chrom, start, end)` → one or more partitions with VFO ranges
    - [ ] No index or no region: single partition `[0, MAX_VALUE]`

- [ ] `BedPartitionReaderFactory.java`, `BedTable.java`, `BedDataSource.java` — standard boilerplate

- [ ] `DataSourceRegister` entry: `com.litebfx.bed.BedDataSource`

### Tests

- [ ] `core/src/test/java/com/litebfx/bed/BedDataSourceTest.java`
  - [ ] Schema: 12 fields, correct types
  - [ ] Count matches line count of test `.bed` file
  - [ ] Region query returns expected count (tabix path)
  - [ ] BED3 file: fields beyond `chromEnd` are all null
  - [ ] BED12 file: `blockCount`, `blockSizes`, `blockStarts` populated
  - [ ] No-tabix fallback: single partition, correct count
  - [ ] Gzipped `.bed.gz` without tabix: single partition

---

## Scala module (`scala/`)

### Build configuration

- [ ] `scala/pom.xml` — parent reference, artifactId `lite-bfx-spark-scala_2.13`
  - [ ] `com.litebfx:lite-bfx-spark:${project.version}` as `compile` (merged into fat JAR by shade)
  - [ ] `spark-sql_2.13:${spark.version}` as `provided`
  - [ ] `scala-library:${scala.version}` as `provided`
  - [ ] `scalatest_2.13` as `test`
  - [ ] `scala-maven-plugin 4.9.2`: goals `compile` + `testCompile`, `scalaVersion=${scala.version}`, `-deprecation -feature -Xfatal-warnings`
  - [ ] `maven-shade-plugin`: include `com.litebfx:lite-bfx-spark` in fat JAR; exclude Spark/Hadoop/Scala-library; strip `.SF`/`.DSA`/`.RSA` signatures

### Source files

- [ ] `scala/src/main/scala/com/litebfx/scala/GenomicRegion.scala`
  - [ ] `final case class GenomicRegion(chromosome: String, start: Int, end: Int)`
  - [ ] `require(start > 0)` and `require(end >= start)` in body
  - [ ] `def overlaps(other: GenomicRegion): Boolean`
  - [ ] Companion: `apply(chromosome, position)` — single-locus shorthand
  - [ ] Companion: `wholeChromosome(chromosome)` — returns `GenomicRegion(chromosome, 1, Int.MaxValue)`

- [ ] `scala/src/main/scala/com/litebfx/scala/DataFrameReaderOps.scala`
  - [ ] `implicit class DataFrameReaderOps(val reader: DataFrameReader) extends AnyVal`
  - [ ] `def bam(path: String, indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200, useIndex: Boolean = true): DataFrame`
  - [ ] `def bamRegion(path: String, region: GenomicRegion, indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200): DataFrame` — delegates to `bam(...)` then `filterRegion`
  - [ ] `def cram(path: String, referenceFile: Option[String] = None, referenceMode: String = "file", indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200): DataFrame`
  - [ ] `def fastq(path: String, numPartitions: Int = 200): DataFrame`
  - [ ] `def vcf(path: String, indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200, useIndex: Boolean = true): DataFrame`
  - [ ] `def fasta(path: String, indexPath: Option[String] = None, numPartitions: Int = 200): DataFrame`
  - [ ] `def bed(path: String, indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200, useIndex: Boolean = true): DataFrame`

- [ ] `scala/src/main/scala/com/litebfx/scala/DataFrameOps.scala`
  - [ ] `implicit class DataFrameOps(val df: DataFrame) extends AnyVal`
  - [ ] `def filterRegion(region: GenomicRegion): DataFrame` — filter on `referenceName`, `start >= region.start`, `start <= region.end` (BAM/CRAM)
  - [ ] `def filterRegion(chromosome: String, start: Int, end: Int): DataFrame` — overload delegating to above
  - [ ] `def filterChromosome(chromosome: String): DataFrame` — filter on `referenceName` equality (BAM/CRAM)
  - [ ] `def filterMapped: DataFrame` — filter `(flags & 0x4) === 0` (BAM/CRAM, row-level only)
  - [ ] `def filterMappingQuality(minMQ: Int): DataFrame` — filter `mappingQuality >= minMQ` (BAM/CRAM, row-level only)
  - [ ] `def withoutAttributes: DataFrame` — `df.drop("attributes")` (BAM/CRAM/VCF)
  - [ ] `def filterVariantRegion(chrom: String, start: Int, end: Int): DataFrame` — filter on `chrom`, `pos >= start`, `pos <= end` (VCF)
  - [ ] `def filterVariantRegion(region: GenomicRegion): DataFrame` — overload delegating to above
  - [ ] `def filterBedRegion(chrom: String, start: Long, end: Long): DataFrame` — filter on `chrom`, `chromStart >= start`, `chromEnd <= end` (BED)
  - [ ] `def filterBedRegion(region: GenomicRegion): DataFrame` — overload delegating to above

- [ ] `scala/src/main/scala/com/litebfx/scala/package.scala`
  - [ ] `package object scala` containing `object implicits`
  - [ ] `implicits` has implicit defs converting `DataFrameReader` → `DataFrameReaderOps` and `DataFrame` → `DataFrameOps`

- [ ] `scala/src/main/scala/com/litebfx/scala/LiteBfxSpark.scala`
  - [ ] `object LiteBfxSpark`
  - [ ] `def read(spark: SparkSession, path: String, indexPath: Option[String] = None, indexDir: Option[String] = None, numPartitions: Int = 200, useIndex: Boolean = true): DataFrame`
  - [ ] `def readRegion(spark: SparkSession, path: String, region: GenomicRegion, ...): DataFrame`
  - [ ] `def readCram(spark: SparkSession, path: String, referenceFile: Option[String] = None, ...): DataFrame`
  - [ ] `def readFastq(spark: SparkSession, path: String, numPartitions: Int = 200): DataFrame`
  - [ ] `def readVcf(spark: SparkSession, path: String, indexPath: Option[String] = None, ...): DataFrame`
  - [ ] `def readFasta(spark: SparkSession, path: String, indexPath: Option[String] = None, numPartitions: Int = 200): DataFrame`
  - [ ] `def readBed(spark: SparkSession, path: String, indexPath: Option[String] = None, ...): DataFrame`
  - [ ] `def region(chromosome: String, start: Int, end: Int): GenomicRegion` — convenience factory

### Tests

- [ ] `scala/src/test/resources/range.bam` — copy of core test fixture
- [ ] `scala/src/test/resources/range.bam.bai` — copy of core test fixture
- [ ] `scala/src/test/scala/com/litebfx/scala/DataFrameReaderOpsTest.scala`
  - [ ] `spark.read.bam(path)` schema matches `BamSchema.SCHEMA`
  - [ ] Named options (`indexPath`, `numPartitions`) are passed through correctly
  - [ ] `filterRegion` reduces count to expected value (verifies pushdown fires via count)
  - [ ] `spark.read.cram(path, referenceFile = Some(...))` schema matches `BamSchema.SCHEMA`
  - [ ] `spark.read.fastq(path)` schema matches `FastqSchema.SCHEMA`
  - [ ] `spark.read.vcf(path)` schema matches `VcfSchema.SCHEMA`
  - [ ] `spark.read.fasta(path)` schema matches `FastaSchema.SCHEMA`
  - [ ] `spark.read.bed(path)` schema matches `BedSchema.SCHEMA`
- [ ] `scala/src/test/scala/com/litebfx/scala/DataFrameOpsTest.scala`
  - [ ] `filterRegion("CHROMOSOME_I", 1, 999999)` count = 18 (BAM)
  - [ ] `filterChromosome("CHROMOSOME_II")` count = 34 (BAM)
  - [ ] `filterMapped` count ≤ total count (BAM)
  - [ ] `filterMappingQuality(30)` count ≤ total count (BAM)
  - [ ] `withoutAttributes` result schema lacks `attributes` field (BAM)
  - [ ] `filterVariantRegion("chr1", 1, 1000000)` count matches tabix region query (VCF)
  - [ ] `filterBedRegion("chr1", 0, 1000000)` count matches tabix region query (BED)
- [ ] `scala/src/test/scala/com/litebfx/scala/LiteBfxSparkTest.scala`
  - [ ] `LiteBfxSpark.read` produces same count as `spark.read.bam` via implicits
  - [ ] `LiteBfxSpark.readRegion` produces same count as `.filterRegion` chain
  - [ ] `LiteBfxSpark.readCram` produces correct count with reference file
  - [ ] `LiteBfxSpark.readFastq` produces correct count
  - [ ] `LiteBfxSpark.readVcf` produces correct count
  - [ ] `LiteBfxSpark.readFasta` produces correct contig count
  - [ ] `LiteBfxSpark.readBed` produces correct count
  - [ ] `LiteBfxSpark.region` constructs a valid `GenomicRegion`
