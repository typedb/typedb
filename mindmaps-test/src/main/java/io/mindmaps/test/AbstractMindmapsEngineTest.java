package io.mindmaps.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsGraphFactory;
import io.mindmaps.MindmapsTest;
import io.mindmaps.engine.MindmapsEngineServer;
import org.junit.After;
import org.junit.Before;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public abstract class AbstractMindmapsEngineTest {
    protected static MindmapsGraphFactory factory;
    protected static MindmapsGraph graph;

    private static AtomicBoolean EMBEDDED_CASS_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    public static void startTestEngine() throws Exception {
        if (EMBEDDED_CASS_ON.compareAndSet(false, true)) {
            startEmbeddedCassandra();
        }

        MindmapsEngineServer.stop();
        sleep(5000);
        MindmapsEngineServer.start();
        sleep(5000);
    }

    public static MindmapsGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (MindmapsTest.usingOrientDB()) {
            keyspace = "memory";
        } else {
            keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace);
    }

    public static MindmapsGraph graphWithNewKeyspace() {
        MindmapsGraphFactory factory = factoryWithNewKeyspace();
        return factory.getGraph();
    }

    private static void startEmbeddedCassandra() {
        try {
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            hideLogs();
            sleep(5000);
        }
        catch (ClassNotFoundException ignored) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public abstract void buildGraph();

    @After
    public abstract void clearGraph();
}
