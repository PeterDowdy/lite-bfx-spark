package com.litebfx.scala

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.col

/**
 * Extension methods for [[org.apache.spark.sql.DataFrame]] providing
 * genomics-friendly row-level filter utilities.
 *
 * Brought into scope via `import com.litebfx.scala.implicits._`.
 */
class DataFrameOps(val df: DataFrame) {

  // ---------------------------------------------------------------------------
  // BAM / CRAM filters  (operate on referenceName / start / flags / mappingQuality)
  // ---------------------------------------------------------------------------

  /**
   * Filter BAM/CRAM rows to those whose alignment start falls within the given
   * genomic region (1-based, inclusive).  The referenceName equality also fires
   * BAI/CRAI predicate pushdown when combined with `spark.read.bam(...)`.
   */
  def filterRegion(region: GenomicRegion): DataFrame =
    df.filter(
      col("referenceName") === region.chromosome &&
      col("start") >= region.start &&
      col("start") <= region.end
    )

  /** Overload accepting raw coordinates instead of a [[GenomicRegion]]. */
  def filterRegion(chromosome: String, start: Int, end: Int): DataFrame =
    filterRegion(GenomicRegion(chromosome, start, end))

  /** Filter BAM/CRAM rows to those aligned to the given chromosome. */
  def filterChromosome(chromosome: String): DataFrame =
    df.filter(col("referenceName") === chromosome)

  /** Filter BAM/CRAM rows to mapped reads only (SAM FLAG 0x4 unset). */
  def filterMapped: DataFrame =
    df.filter(col("flags").bitwiseAND(0x4) === 0)

  /** Filter BAM/CRAM rows to reads with mapping quality >= minMQ. */
  def filterMappingQuality(minMQ: Int): DataFrame =
    df.filter(col("mappingQuality") >= minMQ)

  /** Drop the `attributes` column.  Applicable to BAM, CRAM, and VCF DataFrames. */
  def withoutAttributes: DataFrame =
    df.drop("attributes")

}
