package io.mindmaps.test.tinker;

import io.mindmaps.Mindmaps;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.junit.BeforeClass;

import java.util.UUID;

public abstract class MindmapsTinkerTestBase extends AbstractMindmapsEngineTest {
    private static final String CONFIG_FILE =  "../../conf/test/tinker/mindmaps-engine.properties";

    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception {
        startTestEngine(CONFIG_FILE);
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
