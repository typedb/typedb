package io.mindmaps;

import io.mindmaps.engine.util.ConfigProperties;
import org.junit.BeforeClass;

public abstract class MindmapsOrientDBTestBase extends AbstractMindmapsEngineTest {
    @BeforeClass
    public static void startEngine() throws Exception {
        startTestEngine(ConfigProperties.EXPERIMENTAL_CONFIG_FILE);
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
