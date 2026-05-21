package com.litebfx.scala

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.apache.spark.sql.functions.col

/**
 * Extension methods for [[org.apache.spark.sql.DataFrameReader]] that expose
 * genomics file formats supported by lite-bfx-spark.
 *
 * Brought into scope via `import com.litebfx.scala.implicits._`.
 * For non-implicit usage see [[LiteBfxSpark]].
 */
class DataFrameReaderOps(val reader: DataFrameReader) {

  /**
   * Read a BAM or SAM file (auto-detected from content).
   *
   * @param path           File path, directory, or glob (supports s3a://, dbfs:/, etc.)
   * @param indexPath      Explicit BAI path; single-file reads only.
   * @param indexDir       Directory of BAI files; `indexDir/<filename>.bai` per file.
   * @param numPartitions  Max partitions per file when BAI-based splitting is available.
   * @param useIndex       Set false to skip index lookup (forces single partition).
   */
  def bam(
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame = {
    var r = reader.format("bam")
      .option("numPartitions", numPartitions)
      .option("useIndex", useIndex)
    indexPath.foreach(p => r = r.option("indexPath", p))
    indexDir.foreach(d => r = r.option("indexDir", d))
    r.load(path)
  }

  /**
   * Read a BAM file and immediately filter to the given genomic region.
   * The region is also pushed down to the BAI index for partition pruning.
   */
  def bamRegion(
    path: String,
    region: GenomicRegion,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame =
    bam(path, indexPath, indexDir, numPartitions)
      .filter(
        col("referenceName") === region.chromosome &&
        col("start") >= region.start &&
        col("start") <= region.end
      )

  /**
   * Read a CRAM file.
   *
   * @param referenceFile  Path to the FASTA reference (with co-located .fai).
   *                       Required for full sequence decoding.
   * @param referenceMode  "file" (use referenceFile), "md5" (ENA/NCBI lookup), or
   *                       "none" (no reference — bases may be unavailable for some reads).
   */
  def cram(
    path: String,
    referenceFile: Option[String] = None,
    referenceMode: String = "file",
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame = {
    var r = reader.format("cram")
      .option("referenceMode", referenceMode)
      .option("numPartitions", numPartitions)
    referenceFile.foreach(f => r = r.option("referenceFile", f))
    indexPath.foreach(p => r = r.option("indexPath", p))
    indexDir.foreach(d => r = r.option("indexDir", d))
    r.load(path)
  }

  /**
   * Read a FASTQ file (.fastq, .fq, .fastq.gz, .fq.gz).
   * Gzipped files always produce a single partition.
   */
  def fastq(path: String, numPartitions: Int = 200): DataFrame =
    reader.format("fastq").option("numPartitions", numPartitions).load(path)

  /**
   * Read a VCF or BCF file (.vcf, .vcf.gz, .bcf).
   * Tabix (.tbi) or CSI (.csi) indices enable region-based partition pruning.
   */
  def vcf(
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame = {
    var r = reader.format("vcf")
      .option("numPartitions", numPartitions)
      .option("useIndex", useIndex)
    indexPath.foreach(p => r = r.option("indexPath", p))
    indexDir.foreach(d => r = r.option("indexDir", d))
    r.load(path)
  }

  /**
   * Read a FASTA file (.fa, .fasta).
   * With a co-located .fai index, each contig becomes one Spark partition.
   */
  def fasta(
    path: String,
    indexPath: Option[String] = None,
    numPartitions: Int = 200
  ): DataFrame = {
    var r = reader.format("fasta").option("numPartitions", numPartitions)
    indexPath.foreach(p => r = r.option("indexPath", p))
    r.load(path)
  }

  /**
   * Read a BED file (.bed, .bed.gz).
   * Tabix or CSI indices enable region-based partition pruning.
   */
  def bed(
    path: String,
    indexPath: Option[String] = None,
    indexDir: Option[String] = None,
    numPartitions: Int = 200,
    useIndex: Boolean = true
  ): DataFrame = {
    var r = reader.format("bed")
      .option("numPartitions", numPartitions)
      .option("useIndex", useIndex)
    indexPath.foreach(p => r = r.option("indexPath", p))
    indexDir.foreach(d => r = r.option("indexDir", d))
    r.load(path)
  }
}
