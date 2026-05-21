package com.litebfx.scala

import com.litebfx.bam.{BamSchema, CramDataSourceTest, TestBamGenerator}
import com.litebfx.bed.{BedSchema, BedTestGenerator}
import com.litebfx.fasta.FastaSchema
import com.litebfx.fastq.{FastqSchema, FastqTestGenerator}
import com.litebfx.vcf.{VcfSchema, VcfTestGenerator}
import com.litebfx.scala.implicits._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/**
 * Integration tests for [[DataFrameReaderOps]] — verifies that each format's
 * `spark.read.<format>(path, ...)` method produces the correct schema and that
 * named options are threaded through correctly.
 */
class DataFrameReaderOpsTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var tempDir: Path = _

  // paths resolved in beforeAll
  var bamPath: String = _
  var baiPath: String = _
  var cramPath: String = _
  var fastaRefPath: String = _
  var fastaPath: String = _
  var fastqPath: String = _
  var vcfPath: String = _
  var bedPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFrameReaderOpsTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("scala-reader-test")

    // BAM
    val bamUrl = getClass.getClassLoader.getResource("range.bam")
    require(bamUrl != null, "range.bam not found on test classpath")
    bamPath = java.nio.file.Paths.get(bamUrl.toURI).toUri.toString
    val baiUrl = getClass.getClassLoader.getResource("range.bam.bai")
    require(baiUrl != null, "range.bam.bai not found on test classpath")
    baiPath = java.nio.file.Paths.get(baiUrl.toURI).toUri.toString

    // CRAM (generated synthetic)
    val faPath = CramDataSourceTest.generateFasta(tempDir)
    fastaRefPath = faPath.toAbsolutePath.toString
    cramPath = CramDataSourceTest.generateCram(tempDir, faPath).toUri.toString

    // FASTA
    val fastaUrl = getClass.getClassLoader.getResource("realn01.fa")
    require(fastaUrl != null, "realn01.fa not found on test classpath")
    fastaPath = java.nio.file.Paths.get(fastaUrl.toURI).toUri.toString

    // FASTQ (generated)
    val fqFx = FastqTestGenerator.generate(tempDir)
    fastqPath = fqFx.plainFastq.toString

    // VCF (generated bgzipped + tabix)
    val vcfFx = VcfTestGenerator.generate(tempDir)
    vcfPath = vcfFx.plainVcf.toString

    // BED
    val bedUrl = getClass.getClassLoader.getResource("example.bed.gz")
    require(bedUrl != null, "example.bed.gz not found on test classpath")
    bedPath = java.nio.file.Paths.get(bedUrl.toURI).toUri.toString
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteDir(tempDir.toFile)
  }

  // ---------------------------------------------------------------------------
  // BAM
  // ---------------------------------------------------------------------------

  test("bam: schema matches BamSchema.SCHEMA") {
    val df = spark.read.bam(bamPath)
    assert(df.schema == BamSchema.SCHEMA)
  }

  test("bam: full-file count = 112") {
    assert(spark.read.bam(bamPath).count() == 112L)
  }

  test("bam: explicit indexPath option filters correctly") {
    val count = spark.read.bam(bamPath, indexPath = Some(baiPath))
      .filter("referenceName = 'CHROMOSOME_I'")
      .count()
    assert(count == 18L)
  }

  test("bam: useIndex=false returns all records") {
    assert(spark.read.bam(bamPath, useIndex = false).count() == 112L)
  }

  test("bamRegion: filters to correct count via index pushdown") {
    val count = spark.read.bamRegion(bamPath, GenomicRegion("CHROMOSOME_I", 1, 999999)).count()
    assert(count == 18L)
  }

  // ---------------------------------------------------------------------------
  // CRAM
  // ---------------------------------------------------------------------------

  test("cram: schema matches BamSchema.SCHEMA") {
    val df = spark.read.cram(cramPath, referenceFile = Some(fastaRefPath))
    assert(df.schema == BamSchema.SCHEMA)
  }

  test("cram: count matches record count") {
    val count = spark.read.cram(cramPath, referenceFile = Some(fastaRefPath)).count()
    assert(count == TestBamGenerator.RECORD_COUNT.toLong)
  }

  // ---------------------------------------------------------------------------
  // FASTQ
  // ---------------------------------------------------------------------------

  test("fastq: schema matches FastqSchema.SCHEMA") {
    val df = spark.read.fastq(fastqPath)
    assert(df.schema == FastqSchema.SCHEMA)
  }

  test("fastq: count matches expected") {
    assert(spark.read.fastq(fastqPath).count() == FastqTestGenerator.PLAIN_COUNT.toLong)
  }

  // ---------------------------------------------------------------------------
  // VCF
  // ---------------------------------------------------------------------------

  test("vcf: schema matches VcfSchema.SCHEMA") {
    val df = spark.read.vcf(vcfPath)
    assert(df.schema == VcfSchema.SCHEMA)
  }

  test("vcf: count matches expected") {
    assert(spark.read.vcf(vcfPath).count() == VcfTestGenerator.VCF_TOTAL.toLong)
  }

  // ---------------------------------------------------------------------------
  // FASTA
  // ---------------------------------------------------------------------------

  test("fasta: schema matches FastaSchema.SCHEMA") {
    val df = spark.read.fasta(fastaPath)
    assert(df.schema == FastaSchema.SCHEMA)
  }

  test("fasta: contig count = 1 (realn01.fa has one contig)") {
    assert(spark.read.fasta(fastaPath).count() == 1L)
  }

  // ---------------------------------------------------------------------------
  // BED
  // ---------------------------------------------------------------------------

  test("bed: schema matches BedSchema.SCHEMA") {
    val df = spark.read.bed(bedPath)
    assert(df.schema == BedSchema.SCHEMA)
  }

  test("bed: count of example.bed.gz = 3432") {
    assert(spark.read.bed(bedPath).count() == 3432L)
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def deleteDir(dir: java.io.File): Unit = {
    Option(dir.listFiles()).foreach(_.foreach(deleteDir))
    dir.delete()
  }
}
