package com.litebfx.scala

import com.litebfx.bam.{CramDataSourceTest, TestBamGenerator}
import com.litebfx.bed.BedTestGenerator
import com.litebfx.fastq.FastqTestGenerator
import com.litebfx.vcf.VcfTestGenerator
import com.litebfx.scala.implicits._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/**
 * Integration tests for [[LiteBfxSpark]] — the explicit (non-implicit) API.
 *
 * Each test verifies that the `LiteBfxSpark.*` methods produce the same
 * results as the implicit extension-method equivalents.
 */
class LiteBfxSparkTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var tempDir: Path = _

  var bamPath: String = _
  var cramPath: String = _
  var fastaRefPath: String = _
  var fastaPath: String = _
  var fastqPath: String = _
  var vcfPath: String = _
  var bedPath: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("LiteBfxSparkTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("scala-litebfxspark-test")

    val bamUrl = getClass.getClassLoader.getResource("range.bam")
    require(bamUrl != null, "range.bam not found on test classpath")
    bamPath = java.nio.file.Paths.get(bamUrl.toURI).toUri.toString

    val faPath = CramDataSourceTest.generateFasta(tempDir)
    fastaRefPath = faPath.toAbsolutePath.toString
    cramPath = CramDataSourceTest.generateCram(tempDir, faPath).toUri.toString

    val fastaUrl = getClass.getClassLoader.getResource("realn01.fa")
    require(fastaUrl != null, "realn01.fa not found on test classpath")
    fastaPath = java.nio.file.Paths.get(fastaUrl.toURI).toUri.toString

    val fqFx = FastqTestGenerator.generate(tempDir)
    fastqPath = fqFx.plainFastq.toString

    val vcfFx = VcfTestGenerator.generate(tempDir)
    vcfPath = vcfFx.bgzVcf.toString

    val bedFx = BedTestGenerator.generate(tempDir)
    bedPath = bedFx.bed6Bgzf.toString
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteDir(tempDir.toFile)
  }

  // ---------------------------------------------------------------------------
  // region factory
  // ---------------------------------------------------------------------------

  test("region: constructs a valid GenomicRegion") {
    val r = LiteBfxSpark.region("chr1", 1000, 5000)
    assert(r.chromosome == "chr1")
    assert(r.start == 1000)
    assert(r.end == 5000)
    assert(r.isInstanceOf[GenomicRegion])
  }

  // ---------------------------------------------------------------------------
  // read (BAM)
  // ---------------------------------------------------------------------------

  test("read: count matches spark.read.bam implicit") {
    val explicit = LiteBfxSpark.read(spark, bamPath).count()
    val implicit_ = spark.read.bam(bamPath).count()
    assert(explicit == implicit_)
    assert(explicit == 112L)
  }

  // ---------------------------------------------------------------------------
  // readRegion (BAM)
  // ---------------------------------------------------------------------------

  test("readRegion: count matches plain filter chain") {
    val region    = LiteBfxSpark.region("CHROMOSOME_I", 1, 999999)
    val explicit  = LiteBfxSpark.readRegion(spark, bamPath, region).count()
    val chained   = spark.read.bam(bamPath)
      .filter("referenceName = 'CHROMOSOME_I' AND start >= 1 AND start <= 999999")
      .count()
    assert(explicit == chained)
    assert(explicit == 18L)
  }

  // ---------------------------------------------------------------------------
  // readCram
  // ---------------------------------------------------------------------------

  test("readCram: count matches RECORD_COUNT with referenceFile") {
    val count = LiteBfxSpark.readCram(spark, cramPath,
      referenceFile = Some(fastaRefPath)).count()
    assert(count == TestBamGenerator.RECORD_COUNT.toLong)
  }

  // ---------------------------------------------------------------------------
  // readFastq
  // ---------------------------------------------------------------------------

  test("readFastq: count matches expected") {
    val count = LiteBfxSpark.readFastq(spark, fastqPath).count()
    assert(count == FastqTestGenerator.PLAIN_COUNT.toLong)
  }

  // ---------------------------------------------------------------------------
  // readVcf
  // ---------------------------------------------------------------------------

  test("readVcf: count matches expected") {
    val count = LiteBfxSpark.readVcf(spark, vcfPath).count()
    assert(count == VcfTestGenerator.VCF_TOTAL.toLong)
  }

  // ---------------------------------------------------------------------------
  // readFasta
  // ---------------------------------------------------------------------------

  test("readFasta: contig count = 1") {
    val count = LiteBfxSpark.readFasta(spark, fastaPath).count()
    assert(count == 1L)
  }

  // ---------------------------------------------------------------------------
  // readBed
  // ---------------------------------------------------------------------------

  test("readBed: count matches BED6_TOTAL") {
    val count = LiteBfxSpark.readBed(spark, bedPath).count()
    assert(count == BedTestGenerator.BED6_TOTAL.toLong)
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def deleteDir(dir: java.io.File): Unit = {
    Option(dir.listFiles()).foreach(_.foreach(deleteDir))
    dir.delete()
  }
}
