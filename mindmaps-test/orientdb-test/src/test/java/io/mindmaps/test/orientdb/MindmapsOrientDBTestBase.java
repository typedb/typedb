package io.mindmaps.test.orientdb;

import io.mindmaps.Mindmaps;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.junit.BeforeClass;

public abstract class MindmapsOrientDBTestBase extends AbstractMindmapsEngineTest {
    private static final String CONFIG_FILE = "../../conf/test/orientdb/mindmaps-engine.properties";

    @BeforeClass
    public static void startEngine() throws Exception {
        startTestEngine(CONFIG_FILE);
    }

    @Override
    public void buildGraph(){
        factory = Mindmaps.factory(Mindmaps.DEFAULT_URI, "memory");
        graph = factory.getGraph();
    }
    @Override
    public void clearGraph(){
        graph.clear();
    }
}
