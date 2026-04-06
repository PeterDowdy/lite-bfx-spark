package com.litebfx.fasta;

import com.litebfx.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * Describes a single Spark partition over a FASTA file.
 *
 * <p>When a FAI index is present, one partition is created per contig and
 * {@code contigName} is set to the contig identifier; the reader uses
 * {@code IndexedFastaSequenceFile.getSequence(contigName)} for O(1) access.
 *
 * <p>When no FAI is available, a single partition is created with
 * {@code contigName=null}; the reader iterates all sequences via
 * {@code nextSequence()}.
 */
public class FastaInputPartition implements InputPartition, Serializable {

    private final String path;
    /** Contig to read; null means "iterate all contigs in order". */
    private final String contigName;
    private final SerializableConfiguration hadoopConf;

    public FastaInputPartition(String path, String contigName, Configuration hadoopConf) {
        this.path = path;
        this.contigName = contigName;
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    public String getPath() { return path; }
    public String getContigName() { return contigName; }
    public Configuration getHadoopConf() { return hadoopConf.get(); }
}
