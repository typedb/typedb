package io.mindmaps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.engine.util.ConfigProperties;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public abstract class MindmapsTitanTestBase extends AbstractMindmapsEngineTest {
    private static AtomicBoolean EMBEDDED_CASS_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception {
        startTestEngine(ConfigProperties.EMBEDDED_CONFIG_FILE);

        if (EMBEDDED_CASS_ON.compareAndSet(false, true)) {
            hideLogs();
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
            hideLogs();
            sleep(5000);
        }
    }

    @Override
    public void buildGraph(){
        graph = graphWithNewKeyspace();
    }
    @Override
    public void clearGraph(){
        graph.clear();
    }
}
