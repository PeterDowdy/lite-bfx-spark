import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Validates that the <em>packaged, shaded</em> {@code lite-bfx-spark} fat JAR is
 * self-contained: every format reader must work using only the relocated htsjdk
 * classes bundled inside the JAR.
 *
 * <p>This is the regression guard for shrinking the published artifact. The smoke
 * module depends on {@code lite-bfx-spark} with {@code com.github.samtools:htsjdk}
 * <b>excluded</b> (see {@code tests/smoke/pom.xml}), so the only htsjdk-derived
 * classes on the classpath are the shaded ones inside the fat JAR. If a shade
 * exclusion or {@code minimizeJar} strips a class a reader needs at runtime (e.g. a
 * CRAM/tabix/VCF codec loaded reflectively), the corresponding read throws
 * {@link NoClassDefFoundError} and the matching test below fails — something the
 * core module's own tests cannot catch, because they compile against the full,
 * unshaded htsjdk.
 *
 * <p>Fixtures: text/binary reader fixtures come from the core module's test
 * resources (path passed via the {@code fixtures.dir} system property); the
 * reference-free CRAM and the plain VCF live in this module's own resources.
 */
public class FatJarReaderTest {

    private static SparkSession spark;

    /** core/src/test/resources, supplied by surefire (see pom). */
    private static String coreFixtures;

    @BeforeClass
    public static void setUp() {
        coreFixtures = System.getProperty("fixtures.dir");
        assertNotNull("fixtures.dir system property must be set by surefire", coreFixtures);
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("lite-bfx-spark-fatjar")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    /** A fixture in the core module's test resources. */
    private static String coreFixture(String name) {
        File f = new File(coreFixtures, name);
        assertTrue("core fixture missing: " + f, f.exists());
        return f.toURI().toString();
    }

    /** A fixture bundled in this module's own resources (copied to target/test-classes). */
    private static String localFixture(String name) throws Exception {
        URL url = FatJarReaderTest.class.getClassLoader().getResource(name);
        assertNotNull("local fixture missing: " + name, url);
        return Paths.get(url.toURI()).toUri().toString();
    }

    private long count(String format, String path) {
        return spark.read().format(format).load(path).count();
    }

    // -- BGZF + BAI index path (htsjdk.samtools.*) --------------------------

    @Test
    public void bam_readsAllRecords() {
        assertEquals(112L, count("bam", coreFixture("range.bam")));
    }

    // -- SAM text path (htsjdk.samtools.SAMTextReader) ----------------------

    @Test
    public void sam_readsAllRecords() {
        assertEquals(9L, count("bam", coreFixture("realn02-r.sam")));
    }

    // -- CRAM codec path (htsjdk.samtools.cram.*), reference-free -----------

    @Test
    public void cram_readsAllRecords() throws Exception {
        assertEquals(112L, count("cram", localFixture("range_noref.cram")));
    }

    // -- FASTA + FAI (htsjdk.samtools.reference.*) --------------------------

    @Test
    public void fasta_readsAllContigs() {
        assertEquals(1L, count("fasta", coreFixture("realn01.fa")));
    }

    // -- FASTQ (htsjdk.samtools.fastq.*) ------------------------------------

    @Test
    public void fastq_readsAllRecords() {
        assertEquals(25000L, count("fastq", coreFixture("TESTX_H7YRLADXX_S1_L001_R1_001.fastq.gz")));
    }

    // -- BED (BGZF read path) ----------------------------------------------

    @Test
    public void bed_readsAllRecords() {
        assertEquals(3432L, count("bed", coreFixture("example.bed.gz")));
    }

    // -- VCF codec path (htsjdk.variant.*, htsjdk.tribble.*) ----------------

    @Test
    public void vcf_readsAllRecords() throws Exception {
        assertEquals(3L, count("vcf", localFixture("sample.vcf")));
    }
}
