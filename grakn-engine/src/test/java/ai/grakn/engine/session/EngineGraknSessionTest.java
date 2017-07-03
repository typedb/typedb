package ai.grakn.engine.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;

public class EngineGraknSessionTest {
    
    static { EngineTestHelper.engine(); }
    static EngineGraknGraphFactory graknFactory = EngineGraknGraphFactory.create(EngineTestHelper.config().getProperties());
    
    String factoryUri = "localhost:" + EngineTestHelper.config().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER);
    
    @Test
    public void testDifferentFactoriesReturnTheSameGraph(){
        String keyspace = "mykeyspace";

        GraknGraph graph1 = Grakn.session(factoryUri, keyspace).open(GraknTxType.WRITE);
        graph1.close();
        GraknGraph graph2 = graknFactory.getGraph(keyspace, GraknTxType.WRITE);

        assertEquals(graph1, graph2);
        graph2.close();
    }

    @Test
    public void testBatchLoadingGraphsInitialisedCorrectly(){
        String keyspace = "mykeyspace";
        GraknGraph graph1 = graknFactory.getGraph(keyspace, GraknTxType.WRITE);
        graph1.close();
        GraknGraph graph2 = graknFactory.getGraph(keyspace, GraknTxType.BATCH);

        assertFalse(graph1.admin().isBatchGraph());
        assertTrue(graph2.admin().isBatchGraph());

        graph1.close();
        graph2.close();
    }
}
