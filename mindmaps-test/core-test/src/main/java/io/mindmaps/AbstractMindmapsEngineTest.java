package io.mindmaps;

import io.mindmaps.engine.MindmapsEngineServer;
import io.mindmaps.engine.util.ConfigProperties;
import org.javatuples.Pair;

import java.util.UUID;

import static java.lang.Thread.sleep;

public abstract class AbstractMindmapsEngineTest {

    public static void startTestEngine(String configPath) throws Exception {
        MindmapsEngineServer.stop();
        sleep(5000);

        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, configPath);
        MindmapsEngineServer.start();
        sleep(5000);
    }

    public static Pair<MindmapsGraph, String> graphWithNewKeyspace() {
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        MindmapsGraph graph = Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace).getGraph();
        return Pair.with(graph, keyspace);
    }
}
