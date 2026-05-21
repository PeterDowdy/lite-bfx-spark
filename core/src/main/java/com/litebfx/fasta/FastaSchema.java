package com.litebfx.fasta;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark schema for a FASTA file.
 *
 * <p>Each row represents one contig (reference sequence):
 * <ul>
 *   <li>{@code name}     — contig identifier (FASTA header without '&gt;')</li>
 *   <li>{@code sequence} — full nucleotide sequence as a String</li>
 *   <li>{@code length}   — sequence length in bases</li>
 * </ul>
 */
public class FastaSchema {

    public static final StructType SCHEMA = new StructType()
        .add("name",     DataTypes.StringType, false)
        .add("sequence", DataTypes.StringType, false)
        .add("length",   DataTypes.LongType,   false);

    private FastaSchema() {}
}
