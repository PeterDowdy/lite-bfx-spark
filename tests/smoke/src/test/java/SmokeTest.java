import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Minimal smoke test: verifies that a SparkSession can be created and
 * execute a basic SQL query. Run this in every Docker target image to
 * confirm the Spark + JVM environment is sane before running full tests.
 */
public class SmokeTest {

    private static SparkSession spark;

    @BeforeClass
    public static void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("lite-bfx-spark-smoke")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void selectOne() {
        Row row = spark.sql("SELECT 1 AS value").first();
        assertEquals("SELECT 1 should return 1", 1, row.getInt(0));
        System.out.println("[SMOKE] SELECT 1 passed on: " + spark.version());
    }

    @Test
    public void sparkVersion() {
        String version = spark.version();
        System.out.println("[SMOKE] Spark version: " + version);
        // Assert we are on a 4.x (or later) build
        int major = Integer.parseInt(version.split("\\.")[0]);
        assertEquals("Expected Spark 4.x", 4, major);
    }
}
