package ai.grakn.test.engine.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.EngineGraknGraph;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EngineGraknGraphFactoryTest {
    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Test
    public void testDifferentFactoriesReturnTheSameGraph(){
        String keyspace = "mykeyspace";

        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        EngineGraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(keyspace);

        assertEquals(graph1, graph2);
    }

    @Test
    public void testBatchLoadingGraphsInitialisedCorrectly(){
        String keyspace = "mykeyspace";
        EngineGraknGraph graph1 = EngineGraknGraphFactory.getInstance().getGraph(keyspace);
        EngineGraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraphBatchLoading(keyspace);

        assertFalse(graph1.admin().isBatchLoadingEnabled());
        assertTrue(graph2.admin().isBatchLoadingEnabled());
    }
}
