package io.mindmaps.factory;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;
import io.mindmaps.core.implementation.MindmapsTinkerTransaction;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class MindmapsTinkerGraphFactoryTest {
    private MindmapsGraphFactory tinkerGraphFactory;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        tinkerGraphFactory = new MindmapsTinkerGraphFactory();
    }

    @Test
    public void testBuildTinkerGraph() throws Exception {
        MindmapsGraph graph = tinkerGraphFactory.getGraph("test", null, null);
        MindmapsTransaction transaction = graph.newTransaction();
        assertThat(graph, instanceOf(MindmapsTinkerGraph.class));
        assertThat(transaction, instanceOf(MindmapsTinkerTransaction.class));;

        try {
            transaction.close();
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFactoryMap(){
        MindmapsGraph graph1 = tinkerGraphFactory.getGraph("graph1", null, null);
        MindmapsGraph graph2 = tinkerGraphFactory.getGraph("graph2", null, null);
        MindmapsGraph graph1_copy = tinkerGraphFactory.getGraph("graph1", null, null);

        assertNotEquals(graph1, graph2);
        assertEquals(graph1, graph1_copy);
    }
}