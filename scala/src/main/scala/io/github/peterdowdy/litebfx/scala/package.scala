package io.github.peterdowdy.litebfx

import org.apache.spark.sql.DataFrameReader

/**
 * Scala API for lite-bfx-spark.
 *
 * A single import brings reader extension methods into scope:
 * {{{
 *   import io.github.peterdowdy.litebfx.scala.implicits._
 *   import io.github.peterdowdy.litebfx.scala.GenomicRegion
 *
 *   val df = spark.read.bam("s3a://bucket/sample.bam")
 *   df.filter("referenceName = 'chr1' AND start >= 1000 AND start <= 2000").show()
 * }}}
 *
 * For non-implicit usage see [[LiteBfxSpark]].
 */
package object scala {

  /**
   * Single opt-in import that wires the DataFrameReader implicit conversion.
   * Mirrors the `spark.implicits._` pattern from Spark itself.
   */
  object implicits {
    import _root_.scala.language.implicitConversions

    implicit def toDataFrameReaderOps(reader: DataFrameReader): DataFrameReaderOps =
      new DataFrameReaderOps(reader)
  }
}
