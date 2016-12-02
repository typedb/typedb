package ai.grakn.factory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public abstract class TitanTestBase {
    final static boolean TEST_BATCH_LOADING = false;
    final static String TEST_CONFIG = "../conf/main/grakn.properties";
    final static String TEST_URI = null;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupCass(){
        try {
            hideLogs();
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
            hideLogs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void killCass(){
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }
}
