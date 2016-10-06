package io.mindmaps;

import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import org.junit.After;
import org.junit.Before;

import java.util.UUID;

import static java.lang.Thread.sleep;

public abstract class AbstractMindmapsEngineTest {
    protected static  MindmapsGraph graph;

    public static void startTestEngine(String configPath) throws Exception {
        MindmapsEngineServer.stop();
        sleep(5000);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, configPath);
        MindmapsEngineServer.start();
        sleep(5000);
    }

    public static MindmapsGraph graphWithNewKeyspace() {
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, UUID.randomUUID().toString().replaceAll("-", "")).getGraph();
    }

    public static MindmapsGraph graphWithNewKeyspace(String keyspace){
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
    }

    @Before
    public abstract void buildGraph();

    @After
    public abstract void clearGraph();
}
