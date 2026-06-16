package io.github.peterdowdy.litebfx.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Explicit (non-implicit) entry point for lite-bfx-spark.
 *
 * Use this when you prefer to avoid wildcard implicit imports, or when calling
 * from Python/Java via the Scala fat JAR.
 *
 * {{{
 *   val df = LiteBfxSpark.read(spark, "s3a://bucket/sample.bam")
 *   val df = LiteBfxSpark.readRegion(spark, "s3a://bucket/sample.bam",
 *              LiteBfxSpark.region("chr1", 1000, 5000))
 * }}}
 */
object LiteBfxSpark {

  /** Convenience factory for [[GenomicRegion]]. */
  def region(chromosome: String, start: Int, end: Int): GenomicRegion =
    GenomicRegion(chromosome, start, end)

  def read(
    spark: SparkSession,
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame =
    new DataFrameReaderOps(spark.read)
      .bam(path, indexPath, indexDir, numPartitions, useIndex)

  def readRegion(
    spark: SparkSession,
    path: String,
    region: GenomicRegion,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame =
    new DataFrameReaderOps(spark.read)
      .bamRegion(path, region, indexPath, indexDir, numPartitions)

  def readCram(
    spark: SparkSession,
    path: String,
    referenceFile: Option[String] = None,
    referenceMode: String = "file",
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame =
    new DataFrameReaderOps(spark.read)
      .cram(path, referenceFile, referenceMode, indexPath, indexDir, numPartitions)

  def readFastq(
    spark: SparkSession,
    path: String,
    numPartitions: Int = 200
  ): DataFrame =
    new DataFrameReaderOps(spark.read).fastq(path, numPartitions)

  def readVcf(
    spark: SparkSession,
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame =
    new DataFrameReaderOps(spark.read)
      .vcf(path, indexPath, indexDir, numPartitions, useIndex)

  def readFasta(
    spark: SparkSession,
    path: String,
    indexPath: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame =
    new DataFrameReaderOps(spark.read).fasta(path, indexPath, numPartitions)

  def readBed(
    spark: SparkSession,
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame =
    new DataFrameReaderOps(spark.read)
      .bed(path, indexPath, indexDir, numPartitions, useIndex)
}
