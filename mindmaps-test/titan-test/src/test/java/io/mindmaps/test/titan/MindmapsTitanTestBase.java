package io.mindmaps.test.titan;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.Mindmaps;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public abstract class MindmapsTitanTestBase extends AbstractMindmapsEngineTest {
    public static final String EMBEDDED_CONFIG_FILE = "../../conf/test/mindmaps-engine-embedded.properties";
    private static AtomicBoolean EMBEDDED_CASS_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception {
        startTestEngine(EMBEDDED_CONFIG_FILE);

        if (EMBEDDED_CASS_ON.compareAndSet(false, true)) {
            hideLogs();
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
            hideLogs();
            sleep(5000);
        }
    }

    @Override
    public void buildGraph(){
        factory = Mindmaps.factory(Mindmaps.DEFAULT_URI, UUID.randomUUID().toString().replaceAll("-", ""));
        graph = factory.getGraph();
    }
    @Override
    public void clearGraph(){
        graph.clear();
    }
}
