package io.github.peterdowdy.litebfx.fastq;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark schema for a FASTQ file.
 *
 * <p>Each row represents one sequencing read:
 * <ul>
 *   <li>{@code readName}      — read identifier (FASTQ header without leading '@')</li>
 *   <li>{@code sequence}      — nucleotide sequence string</li>
 *   <li>{@code baseQualities} — quality scores in ASCII Phred+33 encoding</li>
 *   <li>{@code description}   — optional description from header line after the space;
 *                               null when absent</li>
 *   <li>{@code readNumber}    — 1 for R1 (forward), 2 for R2 (reverse), null when
 *                               the read number cannot be determined from the filename</li>
 * </ul>
 */
public class FastqSchema {

    public static final StructType SCHEMA = new StructType()
        .add("readName",      DataTypes.StringType,  false)
        .add("sequence",      DataTypes.StringType,  false)
        .add("baseQualities", DataTypes.StringType,  false)
        .add("description",   DataTypes.StringType,  true)
        .add("readNumber",    DataTypes.IntegerType, true);

    private FastqSchema() {}
}
