package ai.grakn.test.engine.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GraphFactoryTest {
    @ClassRule
    public static final EngineContext engine = EngineContext.startServer();

    @Test
    public void testDifferentFactoriesReturnTheSameGraph(){
        String keyspace = "mykeyspace";

        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        GraknGraph graph2 = GraphFactory.getInstance().getGraph(keyspace);

        assertEquals(graph1, graph2);
    }

    @Test
    public void testSystemKeyspaceNotSubmittingLogs(){
        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, SystemKeyspace.SYSTEM_GRAPH_NAME).getGraph();
        ResourceType<String> resourceType = graph1.putResourceType("New Resource Type", ResourceType.DataType.STRING);
    }
}
