package io.github.peterdowdy.litebfx.bam;

import io.github.peterdowdy.litebfx.fasta.FastaScanBuilder;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link BamScan} for a BAM/SAM DataSource V2 read.
 *
 * <h3>Predicate pushdown</h3>
 * Recognized predicates ({@code referenceName} equality + {@code start} range) are
 * extracted and stored for use in {@link BamScan#planInputPartitions()}.  Unrecognized
 * predicates are returned unhandled so Spark always applies a post-scan pass for them.
 *
 * <h3>Column pruning</h3>
 * If {@code attributes} is absent from the required schema, the attributes map
 * is not built during row conversion (avoiding per-record tag parsing overhead).
 */
public class BamScanBuilder
        implements ScanBuilder, SupportsPushDownV2Filters, SupportsPushDownLimit,
                   SupportsPushDownRequiredColumns {

    private static final Logger log = LoggerFactory.getLogger(BamScanBuilder.class);

    private final CaseInsensitiveStringMap options;
    private final boolean isCram;

    /**
     * Active alignment region-filter column names, driven by the {@code columnNames} option:
     * {@code referenceName}/{@code start} (descriptive) or {@code rname}/{@code pos} (SAM-spec).
     */
    private final String refColumn;
    private final String startColumn;

    private StructType requiredSchema;
    private boolean includeAttributes = true;
    private boolean includeFileMetadata = false;

    // Extracted region (null referenceName means "no region filter")
    private String pushedReferenceName = null;
    private int pushedStart = 1;
    private int pushedEnd = Integer.MAX_VALUE;
    private int pushedLimit = Integer.MAX_VALUE;
    private Predicate[] handledPredicates = new Predicate[0];

    BamScanBuilder(CaseInsensitiveStringMap options) {
        this(options, false);
    }

    BamScanBuilder(CaseInsensitiveStringMap options, boolean isCram) {
        log.trace("BamScanBuilder(isCram={})", isCram);
        this.options = options;
        this.isCram = isCram;
        boolean samColumnNames = BamSchema.isSamColumnNames(options);
        this.requiredSchema = samColumnNames ? BamSchema.SAM_SCHEMA : BamSchema.SCHEMA;
        // Predicate pushdown must match whichever name set the caller's DataFrame uses.
        this.refColumn = samColumnNames ? "rname" : "referenceName";
        this.startColumn = samColumnNames ? "pos" : "start";
    }

    // -------------------------------------------------------------------------
    // SupportsPushDownV2Filters
    // -------------------------------------------------------------------------

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        log.trace("pushPredicates(predicates={})", (Object) predicates);
        Predicate[] flat = FastaScanBuilder.flatten(predicates);

        // First pass: extract referenceName equality (case-insensitive column name)
        String refName = null;
        Predicate refPredicate = null;
        for (Predicate p : flat) {
            if (FastaScanBuilder.isColumnEqualityIgnoreCase(p, refColumn)) {
                refName = String.valueOf(FastaScanBuilder.literalValue(p));
                refPredicate = p;
                break;
            }
        }

        List<Predicate> handled = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();

        if (refName != null) {
            handled.add(refPredicate);
            int rangeStart = 1;
            int rangeEnd = Integer.MAX_VALUE;
            // Second pass: extract start range for the BAI index query.
            // Range predicates are returned unhandled so Spark post-filters for exactness;
            // BAI overlap queries may return reads that start outside the requested range.
            for (Predicate p : flat) {
                if (p == refPredicate) continue;
                if (FastaScanBuilder.isRangeComparison(p)
                        && startColumn.equalsIgnoreCase(FastaScanBuilder.columnName(p))) {
                    String op = p.name();
                    int val = ((Number) FastaScanBuilder.literalValue(p)).intValue();
                    if (">=".equals(op)) rangeStart = val;
                    else if (">".equals(op)) rangeStart = val + 1;
                    else if ("<=".equals(op)) rangeEnd = val;
                    else if ("<".equals(op)) rangeEnd = val - 1;
                    unhandled.add(p);
                } else {
                    unhandled.add(p);
                }
            }
            pushedReferenceName = refName;
            pushedStart = rangeStart;
            pushedEnd = rangeEnd;
        } else {
            for (Predicate p : flat) unhandled.add(p);
        }

        log.trace("pushPredicates() extracted referenceName={} start={} end={}",
                pushedReferenceName, pushedStart, pushedEnd);
        handledPredicates = handled.toArray(new Predicate[0]);
        return unhandled.toArray(new Predicate[0]);
    }

    @Override
    public Predicate[] pushedPredicates() {
        log.trace("pushedPredicates()");
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
        boolean found = false;
        boolean metadata = false;
        for (StructField f : requiredSchema.fields()) {
            if ("attributes".equals(f.name())) found = true;
            // Spark appends declared metadata columns to the required schema when referenced.
            else if (BamSchema.METADATA_COLUMN_NAME.equals(f.name())) metadata = true;
        }
        this.includeAttributes = found;
        this.includeFileMetadata = metadata;
        log.trace("pruneColumns() includeAttributes={} includeFileMetadata={}", includeAttributes, includeFileMetadata);
    }

    // -------------------------------------------------------------------------
    // ScanBuilder
    // -------------------------------------------------------------------------

    @Override
    public Scan build() {
        log.trace("build()");
        return new BamScan(options, requiredSchema, includeAttributes,
                           pushedReferenceName, pushedStart, pushedEnd, isCram, pushedLimit,
                           includeFileMetadata);
    }
}
