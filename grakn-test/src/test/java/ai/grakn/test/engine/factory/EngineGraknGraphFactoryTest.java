package ai.grakn.test.engine.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.EngineGraknGraph;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}
