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

  /** Filter BAM/CRAM rows to those aligned to the given chromosome. */
  def filterChromosome(chromosome: String): DataFrame =
    df.filter(col("referenceName") === chromosome)

  /** Drop the `attributes` column.  Applicable to BAM, CRAM, and VCF DataFrames. */
  def withoutAttributes: DataFrame =
    df.drop("attributes")

}
