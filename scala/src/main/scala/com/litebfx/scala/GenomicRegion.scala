package com.litebfx.scala

/**
 * A genomic interval on a named chromosome using 1-based, inclusive coordinates.
 * Follows the same convention as BAM (POS field) and VCF (POS column).
 *
 * @param chromosome  Reference sequence name (e.g. "chr1", "CHROMOSOME_I").
 * @param start       Start position, 1-based inclusive.  Must be > 0.
 * @param end         End position, 1-based inclusive.  Must be >= start.
 */
final case class GenomicRegion(chromosome: String, start: Int, end: Int) {
  require(start > 0, s"start must be > 0, got $start")
  require(end >= start, s"end must be >= start, got end=$end start=$start")

  /**
   * Returns true if this region overlaps `other` on the same chromosome.
   * Overlap is defined as sharing at least one base (inclusive).
   */
  def overlaps(other: GenomicRegion): Boolean =
    chromosome == other.chromosome && start <= other.end && end >= other.start
}

object GenomicRegion {

  /**
   * Single-locus shorthand: creates a region spanning exactly one base.
   */
  def apply(chromosome: String, position: Int): GenomicRegion =
    new GenomicRegion(chromosome, position, position)

  /**
   * Whole-chromosome region: [1, Int.MaxValue].
   * Covers all defined positions for the given chromosome.
   */
  def wholeChromosome(chromosome: String): GenomicRegion =
    new GenomicRegion(chromosome, 1, Int.MaxValue)
}
