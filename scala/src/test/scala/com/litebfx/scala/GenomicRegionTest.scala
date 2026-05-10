package com.litebfx.scala

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[GenomicRegion]].
 *
 * No SparkSession required — these are pure value-class tests.
 */
class GenomicRegionTest extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // Primary constructor and accessors
  // ---------------------------------------------------------------------------

  test("apply(chrom, start, end) constructs region with correct accessors") {
    val r = GenomicRegion("chr1", 100, 200)
    assert(r.chromosome == "chr1")
    assert(r.start == 100)
    assert(r.end == 200)
  }

  // ---------------------------------------------------------------------------
  // Companion object factory methods
  // ---------------------------------------------------------------------------

  test("apply(chrom, pos) creates a single-base region") {
    val r = GenomicRegion("chr2", 500)
    assert(r.chromosome == "chr2")
    assert(r.start == 500)
    assert(r.end == 500)
  }

  test("wholeChromosome creates region from 1 to Int.MaxValue") {
    val r = GenomicRegion.wholeChromosome("chrX")
    assert(r.chromosome == "chrX")
    assert(r.start == 1)
    assert(r.end == Int.MaxValue)
  }

  // ---------------------------------------------------------------------------
  // require guards
  // ---------------------------------------------------------------------------

  test("require: start <= 0 throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      GenomicRegion("chr1", 0, 100)
    }
  }

  test("require: negative start throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      GenomicRegion("chr1", -5, 100)
    }
  }

  test("require: end < start throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      GenomicRegion("chr1", 200, 100)
    }
  }

  test("single-base region (start == end) is valid") {
    val r = GenomicRegion("chr1", 42, 42)
    assert(r.start == 42 && r.end == 42)
  }

  // ---------------------------------------------------------------------------
  // overlaps
  // ---------------------------------------------------------------------------

  test("overlaps: same chrom, overlapping intervals returns true") {
    val a = GenomicRegion("chr1", 100, 300)
    val b = GenomicRegion("chr1", 200, 400)
    assert(a.overlaps(b))
    assert(b.overlaps(a))  // symmetric
  }

  test("overlaps: same chrom, touching at one base returns true") {
    val a = GenomicRegion("chr1", 100, 200)
    val b = GenomicRegion("chr1", 200, 300)
    assert(a.overlaps(b))
    assert(b.overlaps(a))
  }

  test("overlaps: same chrom, non-overlapping intervals returns false") {
    val a = GenomicRegion("chr1", 100, 200)
    val b = GenomicRegion("chr1", 201, 300)
    assert(!a.overlaps(b))
    assert(!b.overlaps(a))
  }

  test("overlaps: different chromosomes returns false") {
    val a = GenomicRegion("chr1", 100, 300)
    val b = GenomicRegion("chr2", 100, 300)
    assert(!a.overlaps(b))
  }

  test("overlaps: a contains b returns true") {
    val a = GenomicRegion("chr1", 100, 500)
    val b = GenomicRegion("chr1", 200, 300)
    assert(a.overlaps(b))
    assert(b.overlaps(a))
  }

  // ---------------------------------------------------------------------------
  // Case class: equals, hashCode, toString, copy, unapply
  // ---------------------------------------------------------------------------

  test("equals: same values are equal") {
    val a = GenomicRegion("chr1", 100, 200)
    val b = GenomicRegion("chr1", 100, 200)
    assert(a == b)
  }

  test("equals: different values are not equal") {
    val a = GenomicRegion("chr1", 100, 200)
    val b = GenomicRegion("chr1", 100, 201)
    assert(a != b)
  }

  test("hashCode: equal regions have the same hash") {
    val a = GenomicRegion("chr1", 100, 200)
    val b = GenomicRegion("chr1", 100, 200)
    assert(a.hashCode() == b.hashCode())
  }

  test("toString contains all three fields") {
    val r = GenomicRegion("chr1", 100, 200)
    val s = r.toString
    assert(s.contains("chr1"))
    assert(s.contains("100"))
    assert(s.contains("200"))
  }

  test("copy changes only the specified field") {
    val original = GenomicRegion("chr1", 100, 200)
    val modified = original.copy(end = 999)
    assert(modified.chromosome == "chr1")
    assert(modified.start == 100)
    assert(modified.end == 999)
    // original unchanged
    assert(original.end == 200)
  }

  test("copy with no changes produces an equal region") {
    val r = GenomicRegion("chrM", 1, 16569)
    assert(r.copy() == r)
  }

  test("unapply: pattern match extracts all three fields") {
    val r = GenomicRegion("chr3", 300, 400)
    r match {
      case GenomicRegion(chrom, start, end) =>
        assert(chrom == "chr3")
        assert(start == 300)
        assert(end == 400)
    }
  }

  // ---------------------------------------------------------------------------
  // Boundary positions
  // ---------------------------------------------------------------------------

  test("start = 1 (minimum valid position) is accepted") {
    val r = GenomicRegion("chr1", 1, 100)
    assert(r.start == 1)
  }

  test("end = Int.MaxValue is accepted") {
    val r = GenomicRegion("chr1", 1, Int.MaxValue)
    assert(r.end == Int.MaxValue)
  }

  test("single-base region at Int.MaxValue is valid") {
    val r = GenomicRegion("chr1", Int.MaxValue, Int.MaxValue)
    assert(r.start == Int.MaxValue)
    assert(r.end   == Int.MaxValue)
  }

  test("wholeChromosome start is 1") {
    assert(GenomicRegion.wholeChromosome("chrY").start == 1)
  }

  // ---------------------------------------------------------------------------
  // overlaps — additional edge cases
  // ---------------------------------------------------------------------------

  test("overlaps: self-overlap returns true") {
    val a = GenomicRegion("chr1", 100, 300)
    assert(a.overlaps(a))
  }

  test("overlaps: single-base regions at same position overlap") {
    val a = GenomicRegion("chr1", 50, 50)
    val b = GenomicRegion("chr1", 50, 50)
    assert(a.overlaps(b))
  }

  test("overlaps: single-base regions at adjacent positions do not overlap") {
    val a = GenomicRegion("chr1", 50, 50)
    val b = GenomicRegion("chr1", 51, 51)
    assert(!a.overlaps(b))
    assert(!b.overlaps(a))
  }

  test("overlaps: b fully contained in a returns true (both directions)") {
    val outer = GenomicRegion("chr1", 1, Int.MaxValue)
    val inner = GenomicRegion("chr1", 100, 200)
    assert(outer.overlaps(inner))
    assert(inner.overlaps(outer))
  }

  test("overlaps: same chromosome different non-adjacent regions return false") {
    val a = GenomicRegion("chr1", 1, 99)
    val b = GenomicRegion("chr1", 101, 200)
    assert(!a.overlaps(b))
    assert(!b.overlaps(a))
  }
}
