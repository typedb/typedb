package io.mindmaps.test.orientdb;

import io.mindmaps.AbstractMindmapsEngineTest;
import io.mindmaps.Mindmaps;
import org.junit.BeforeClass;

public abstract class MindmapsOrientDBTestBase extends AbstractMindmapsEngineTest {
    public static final String EXPERIMENTAL_CONFIG_FILE = "../../conf/experimental/mindmaps-engine-test.properties";

    @BeforeClass
    public static void startEngine() throws Exception {
        startTestEngine(EXPERIMENTAL_CONFIG_FILE);
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
