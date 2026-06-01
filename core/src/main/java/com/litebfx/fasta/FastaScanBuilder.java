package com.litebfx.fasta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link FastaScan}.
 *
 * <h3>Filter pushdown</h3>
 * An equality predicate on {@code name} is used to prune which contig partitions are
 * planned — only the matching contig is scheduled as a Spark partition, avoiding reads
 * of unneeded contigs when a FAI index is present.
 *
 * <h3>Column pruning</h3>
 * Column pruning is supported so that executors can skip base-string
 * decoding when {@code sequence} is not in the required schema.
 */
public class FastaScanBuilder
        implements ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownLimit,
                   SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(FastaScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private StructType requiredSchema = FastaSchema.SCHEMA;
    private String pushedName = null;
    private int faiEntryCount = -1;
    private int pushedLimit = Integer.MAX_VALUE;
    private Predicate[] handledPredicates = new Predicate[0];

    FastaScanBuilder(CaseInsensitiveStringMap options) {
        log.trace("FastaScanBuilder(options={})", options);
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownV2Filters
    // -------------------------------------------------------------------------

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        log.trace("pushPredicates(predicates={})", (Object) predicates);
        List<Predicate> handled = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();
        for (Predicate p : flatten(predicates)) {
            if (isColumnEquality(p, "name")) {
                pushedName = String.valueOf(literalValue(p));
                handled.add(p);
                log.trace("pushPredicates() extracted name={}", pushedName);
            } else {
                unhandled.add(p);
            }
        }
        handledPredicates = handled.toArray(new Predicate[0]);
        return unhandled.toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates() {
        return handledPredicates;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownLimit
    // -------------------------------------------------------------------------

    @Override
    public boolean pushLimit(int limit) {
        log.trace("pushLimit({})", limit);
        this.pushedLimit = limit;
        return false;
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownRequiredColumns
    // -------------------------------------------------------------------------

    @Override
    public void pruneColumns(StructType requiredSchema) {
        log.trace("pruneColumns(requiredSchema={})", requiredSchema);
        this.requiredSchema = requiredSchema;
    }

    // -------------------------------------------------------------------------
    // ScanBuilder
    // -------------------------------------------------------------------------

    @Override
    public Scan build() {
        log.trace("build()");
        faiEntryCount = resolveFaiEntryCount();
        return new FastaScan(options, requiredSchema, pushedName, faiEntryCount, pushedLimit);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private int resolveFaiEntryCount() {
        try {
            Configuration conf = SparkSession.builder().getOrCreate()
                    .sessionState().newHadoopConf();
            List<FileStatus> files = FastaScan.resolveFiles(options, conf);
            if (files.isEmpty()) return -1;
            String fastaPath = files.get(0).getPath().toUri().toString();
            String faiPath = FastaScan.resolveFaiPath(options, fastaPath, conf);
            if (faiPath == null) return -1;
            return FastaScan.readContigNames(faiPath, conf).size();
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Returns true when {@code p} is a "col = literal" predicate for the given column name.
     * V2 predicates produced by Spark always have children[0]=column, children[1]=literal.
     */
    public static boolean isColumnEquality(Predicate p, String columnName) {
        return "=".equals(p.name())
                && p.children().length == 2
                && p.children()[0] instanceof NamedReference nr
                && p.children()[1] instanceof Literal
                && nr.fieldNames().length > 0
                && columnName.equals(nr.fieldNames()[0]);
    }

    /** Case-insensitive variant of {@link #isColumnEquality}. */
    public static boolean isColumnEqualityIgnoreCase(Predicate p, String columnName) {
        return "=".equals(p.name())
                && p.children().length == 2
                && p.children()[0] instanceof NamedReference nr
                && p.children()[1] instanceof Literal
                && nr.fieldNames().length > 0
                && columnName.equalsIgnoreCase(nr.fieldNames()[0]);
    }

    /** Returns the literal value from a binary comparison predicate (children[1]). */
    @SuppressWarnings("unchecked")
    public static Object literalValue(Predicate p) {
        return ((Literal<Object>) p.children()[1]).value();
    }

    /** Returns the column name for a binary comparison predicate, or "" if not applicable. */
    public static String columnName(Predicate p) {
        if (p.children().length >= 1 && p.children()[0] instanceof NamedReference nr
                && nr.fieldNames().length > 0) {
            return nr.fieldNames()[0];
        }
        return "";
    }

    public static boolean isRangeComparison(Predicate p) {
        return (">".equals(p.name()) || ">=".equals(p.name())
                || "<".equals(p.name()) || "<=".equals(p.name()))
                && p.children().length == 2
                && p.children()[0] instanceof NamedReference
                && p.children()[1] instanceof Literal;
    }

    public static Predicate[] flatten(Predicate[] predicates) {
        List<Predicate> result = new ArrayList<>();
        for (Predicate p : predicates) {
            flattenInto(p, result);
        }
        return result.toArray(new Predicate[0]);
    }

    private static void flattenInto(Predicate p, List<Predicate> result) {
        if (p instanceof And and) {
            flattenInto(and.left(), result);
            flattenInto(and.right(), result);
        } else {
            result.add(p);
        }
    }
}
