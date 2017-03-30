package ai.grakn.factory;

import ai.grakn.Grakn;
import ai.grakn.util.ErrorMessage;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class TitanTestBase {
    private final static String CONFIG_LOCATION = "../../conf/main/grakn.properties";
    private final static String TEST_SHARED = "shared";
    static TitanInternalFactory titanGraphFactory;
    final static Properties TEST_PROPERTIES = new Properties();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupMain(){
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try (InputStream in = new FileInputStream(CONFIG_LOCATION)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(CONFIG_LOCATION), e);
        }

        titanGraphFactory = new TitanInternalFactory(TEST_SHARED, Grakn.IN_MEMORY, TEST_PROPERTIES);
    }
}
