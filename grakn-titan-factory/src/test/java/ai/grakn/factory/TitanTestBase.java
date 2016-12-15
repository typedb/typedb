package ai.grakn.factory;

import ai.grakn.util.ErrorMessage;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class TitanTestBase {
    final static boolean TEST_BATCH_LOADING = false;
    final static String TEST_CONFIG = "../conf/main/grakn.properties";
    final static String TEST_URI = null;
    final static Properties TEST_PROPERTIES = new Properties();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupMain(){
        try {
            hideLogs();
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
            hideLogs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (InputStream in = new FileInputStream(TEST_CONFIG)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(TEST_CONFIG), e);
        }
    }

    @AfterClass
    public static void takeDownMain(){
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }
}
