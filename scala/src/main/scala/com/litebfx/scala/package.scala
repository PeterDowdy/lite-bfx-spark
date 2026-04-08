package com.litebfx

import org.apache.spark.sql.{DataFrame, DataFrameReader}

/**
 * Scala API for lite-bfx-spark.
 *
 * A single import brings all extension methods into scope:
 * {{{
 *   import com.litebfx.scala.implicits._
 *   import com.litebfx.scala.GenomicRegion
 *
 *   val df = spark.read.bam("s3a://bucket/sample.bam")
 *   df.filterRegion("chr1", 1000, 2000).show()
 * }}}
 *
 * For non-implicit usage see [[LiteBfxSpark]].
 */
package object scala {

  /**
   * Single opt-in import that wires all implicit conversions.
   * Mirrors the `spark.implicits._` pattern from Spark itself.
   */
  object implicits {
    import _root_.scala.language.implicitConversions

    implicit def toDataFrameReaderOps(reader: DataFrameReader): DataFrameReaderOps =
      new DataFrameReaderOps(reader)

    implicit def toDataFrameOps(df: DataFrame): DataFrameOps =
      new DataFrameOps(df)
  }
}
