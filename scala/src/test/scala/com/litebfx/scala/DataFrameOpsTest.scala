package com.litebfx.scala

import com.litebfx.bed.BedTestGenerator
import com.litebfx.bam.BamSchema
import com.litebfx.vcf.VcfTestGenerator
import com.litebfx.scala.implicits._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/**
 * Integration tests for [[DataFrameOps]] extension methods on [[DataFrame]].
 *
 * BAM tests use `range.bam` (112 records, C. elegans), with per-chromosome
 * ground truth from samtools: I=18, II=34, III=41, IV=19.
 */
class DataFrameOpsTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var tempDir: Path = _

  var bamDf: DataFrame = _
  var vcfDf: DataFrame = _
  var bedDf: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFrameOpsTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    tempDir = Files.createTempDirectory("scala-ops-test")

    val bamUrl = getClass.getClassLoader.getResource("range.bam")
    require(bamUrl != null, "range.bam not found on test classpath")
    val bamPath = java.nio.file.Paths.get(bamUrl.toURI).toUri.toString
    bamDf = spark.read.bam(bamPath).cache()

    val vcfFx = VcfTestGenerator.generate(tempDir)
    vcfDf = spark.read.vcf(vcfFx.bgzVcf.toString,
      indexPath = Some(vcfFx.tbiIndex.toString)).cache()

    val bedFx = BedTestGenerator.generate(tempDir)
    bedDf = spark.read.bed(bedFx.bed6Bgzf.toString,
      indexPath = Some(bedFx.bed6Tbi.toString)).cache()
  }

  override def afterAll(): Unit = {
    if (bamDf != null) bamDf.unpersist()
    if (vcfDf != null) vcfDf.unpersist()
    if (bedDf != null) bedDf.unpersist()
    if (spark != null) spark.stop()
    deleteDir(tempDir.toFile)
  }

  // ---------------------------------------------------------------------------
  // BAM region filter (plain .filter — triggers BAI pushdown)
  // ---------------------------------------------------------------------------

  test("bam filter(referenceName, start range): CHROMOSOME_I count = 18") {
    val count = bamDf.filter("referenceName = 'CHROMOSOME_I' AND start >= 1 AND start <= 999999").count()
    assert(count == 18L)
  }

  // ---------------------------------------------------------------------------
  // filterChromosome (BAM)
  // ---------------------------------------------------------------------------

  test("filterChromosome: CHROMOSOME_II count = 34") {
    assert(bamDf.filterChromosome("CHROMOSOME_II").count() == 34L)
  }

  // ---------------------------------------------------------------------------
  // BAM flag and quality filters (plain .filter)
  // ---------------------------------------------------------------------------

  test("filter mapped reads: count <= total count") {
    val total  = bamDf.count()
    val mapped = bamDf.filter("(flags & 4) = 0").count()
    assert(mapped <= total)
    assert(mapped > 0)
  }

  test("filter mapping quality >= 30: count <= total count") {
    val total = bamDf.count()
    val hq    = bamDf.filter("mappingQuality >= 30").count()
    assert(hq <= total)
    assert(hq > 0)
  }

  // ---------------------------------------------------------------------------
  // withoutAttributes (BAM)
  // ---------------------------------------------------------------------------

  test("withoutAttributes: result schema does not contain 'attributes'") {
    val schema = bamDf.withoutAttributes.schema
    assert(!schema.fieldNames.contains("attributes"))
    assert(schema.length == BamSchema.SCHEMA.length - 1)
  }

  // ---------------------------------------------------------------------------
  // VCF region filter (plain .filter — triggers tabix pushdown)
  // ---------------------------------------------------------------------------

  test("vcf filter(chrom, pos range): chr1 count matches expected") {
    val count = vcfDf.filter("chrom = 'chr1' AND pos >= 1 AND pos <= 1000000").count()
    assert(count == VcfTestGenerator.VCF_CHR1_COUNT.toLong)
  }

  test("vcf filter(chrom, pos range from 500): chr1 from pos 500 count matches expected") {
    val count = vcfDf.filter("chrom = 'chr1' AND pos >= 500 AND pos <= 1000000").count()
    assert(count == VcfTestGenerator.VCF_CHR1_FROM_500.toLong)
  }

  // ---------------------------------------------------------------------------
  // BED region filter (plain .filter — triggers tabix pushdown)
  // ---------------------------------------------------------------------------

  test("bed filter(chrom, chromStart, chromEnd): chr1 count matches expected") {
    val count = bedDf.filter("chrom = 'chr1' AND chromStart >= 0 AND chromEnd <= 1000000").count()
    assert(count == BedTestGenerator.BED6_CHR1_COUNT.toLong)
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def deleteDir(dir: java.io.File): Unit = {
    Option(dir.listFiles()).foreach(_.foreach(deleteDir))
    dir.delete()
  }
}
