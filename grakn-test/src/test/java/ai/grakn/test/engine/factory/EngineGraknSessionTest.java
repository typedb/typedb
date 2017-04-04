package ai.grakn.test.engine.factory;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EngineGraknSessionTest {
    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Test
    public void testDifferentFactoriesReturnTheSameGraph(){
        String keyspace = "mykeyspace";

        GraknGraph graph1 = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE);
        graph1.close();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE);

        assertEquals(graph1, graph2);
        graph2.close();
    }

    @Test
    public void testBatchLoadingGraphsInitialisedCorrectly(){
        String keyspace = "mykeyspace";
        GraknGraph graph1 = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE);
        graph1.close();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.BATCH);

        assertFalse(graph1.admin().isBatchLoadingEnabled());
        assertTrue(graph2.admin().isBatchLoadingEnabled());

        graph1.close();
        graph2.close();
    }
}
