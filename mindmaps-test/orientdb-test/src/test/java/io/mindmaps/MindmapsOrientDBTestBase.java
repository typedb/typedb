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
        graph = graphWithNewKeyspace("memory");
    }
    @Override
    public void clearGraph(){
        graph.clear();
    }
}
