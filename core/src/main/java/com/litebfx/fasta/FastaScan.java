package com.litebfx.fasta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans one {@link FastaInputPartition} per contig when a FAI index is found,
 * or a single full-file partition when no index is available.
 *
 * <h3>FAI resolution order</h3>
 * <ol>
 *   <li>{@code indexPath} option</li>
 *   <li>Co-located {@code <fastaPath>.fai}</li>
 *   <li>None → single partition, full sequential scan</li>
 * </ol>
 */
public class FastaScan implements Scan, Batch {

    private static final Logger log = LoggerFactory.getLogger(FastaScan.class);

    private final CaseInsensitiveStringMap options;
    private final StructType requiredSchema;
    /** When non-null, only create a partition for the contig whose name matches this value. */
    private final String pushedName;

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema) {
        this(options, requiredSchema, null);
    }

    FastaScan(CaseInsensitiveStringMap options, StructType requiredSchema, String pushedName) {
        log.trace("FastaScan(options={}, pushedName={})", options, pushedName);
        this.options = options;
        this.requiredSchema = requiredSchema;
        this.pushedName = pushedName;
    }

    @Override
    public StructType readSchema() {
        return FastaSchema.SCHEMA;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String pathStr = options.get("path");
        log.trace("planInputPartitions() path={}", pathStr);
        if (pathStr == null) {
            throw new IllegalArgumentException("'path' option is required for the fasta data source");
        }

        Configuration hadoopConf = SparkSession.builder().getOrCreate()
                .sessionState().newHadoopConf();

        try {
            int maxMergeGap = FastaInputPartition.DEFAULT_MAX_MERGE_GAP;
            String maxMergeGapOpt = options.get("maxMergeGap");
            if (maxMergeGapOpt != null) {
                maxMergeGap = Integer.parseInt(maxMergeGapOpt);
                log.trace("planInputPartitions() maxMergeGap={}", maxMergeGap);
            }

            String faiPath = resolveFaiPath(pathStr, hadoopConf);
            if (faiPath != null) {
                List<String> contigNames = readContigNames(faiPath, hadoopConf);
                if (pushedName != null) {
                    contigNames = contigNames.stream()
                            .filter(pushedName::equals)
                            .collect(java.util.stream.Collectors.toList());
                    log.trace("planInputPartitions() name filter applied, {} matching contig(s)", contigNames.size());
                } else {
                    log.trace("planInputPartitions() FAI found, {} contig(s)", contigNames.size());
                }
                InputPartition[] partitions = new InputPartition[contigNames.size()];
                for (int i = 0; i < contigNames.size(); i++) {
                    partitions[i] = new FastaInputPartition(pathStr, contigNames.get(i), faiPath, hadoopConf, maxMergeGap);
                }
                return partitions;
            } else {
                log.trace("planInputPartitions() no FAI found, single full-scan partition");
                return new InputPartition[]{new FastaInputPartition(pathStr, null, null, hadoopConf, maxMergeGap)};
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan FASTA input partitions for path: " + pathStr, e);
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.trace("createReaderFactory()");
        return new FastaPartitionReaderFactory();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String resolveFaiPath(String fastaPath, Configuration conf) throws IOException {
        // 1. Explicit indexPath option
        String explicit = options.get("indexPath");
        if (explicit != null) {
            Path p = new Path(explicit);
            if (p.getFileSystem(conf).exists(p)) {
                log.trace("resolveFaiPath() -> explicit indexPath={}", explicit);
                return explicit;
            }
        }

        // 2. Co-located <fastaPath>.fai
        Path colocated = new Path(fastaPath + ".fai");
        FileSystem fs = colocated.getFileSystem(conf);
        if (fs.exists(colocated)) {
            log.trace("resolveFaiPath() -> co-located fai={}", colocated);
            return colocated.toUri().toString();
        }

        log.trace("resolveFaiPath() -> no FAI found");
        return null;
    }

    /**
     * Parses contig names from a FAI file.
     * FAI format: NAME\tLENGTH\tOFFSET\tBASES_PER_LINE\tBYTES_PER_LINE
     */
    private static List<String> readContigNames(String faiPath, Configuration conf) throws IOException {
        log.trace("readContigNames(faiPath={})", faiPath);
        Path path = new Path(faiPath);
        List<String> names = new ArrayList<>();
        try (FSDataInputStream in = path.getFileSystem(conf).open(path);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    names.add(line.split("\t", 2)[0]);
                }
            }
        }
        log.trace("readContigNames() -> {} contig(s)", names.size());
        return names;
    }
}
