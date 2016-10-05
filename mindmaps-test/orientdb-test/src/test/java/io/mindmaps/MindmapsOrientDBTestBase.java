package io.mindmaps;

import io.mindmaps.engine.util.ConfigProperties;
import org.junit.BeforeClass;

public abstract class MindmapsOrientDBTestBase extends AbstractMindmapsEngineTest {
    @BeforeClass
    public static void startEmbeddedCassandra() throws Exception {
        startTestEngine(ConfigProperties.EXPERIMENTAL_CONFIG_FILE);
    }
}
