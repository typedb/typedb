package io.mindmaps.graph.internal;

import io.mindmaps.Mindmaps;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class GraphTestBase {
    protected AbstractMindmapsGraph mindmapsGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUpGraph() {
        mindmapsGraph = (AbstractMindmapsGraph) Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
    }

    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsGraph.close();
    }
}
