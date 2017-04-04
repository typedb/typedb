package ai.grakn.test.graph;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknTinkerGraph;
import ai.grakn.test.EngineContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class GraphTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void isClosedTest() throws Exception {
        GraknGraph graph = engine.factoryWithNewKeyspace().open(GraknTxType.WRITE);
        String keyspace = graph.getKeyspace();
        graph.putEntityType("thing");
        graph.commit();
        assertTrue(graph.isClosed());

        HashSet<Future> futures = new HashSet<>();
        futures.add(Executors.newCachedThreadPool().submit(() -> addThingToBatch(keyspace)));

        for (Future future : futures) {
            future.get();
        }

        assertTrue(graph.isClosed());
    }

    private void addThingToBatch(String keyspace){
        try(GraknGraph graphBatchLoading = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)) {
            graphBatchLoading.getEntityType("thing").addEntity();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSameGraphs() throws GraknValidationException {
        String key = "mykeyspace";
        GraknGraph graph1 = Grakn.session(Grakn.DEFAULT_URI, key).open(GraknTxType.WRITE);
        graph1.close();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(key, GraknTxType.WRITE);
        assertEquals(graph1, graph2);
        graph1.close();
        graph2.close();
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        GraknSession factory = engine.factoryWithNewKeyspace();

        GraknGraph graph = factory.open(GraknTxType.READ);
        graph.close();
        GraknGraph batchGraph = factory.open(GraknTxType.BATCH);

        for(int i = 0; i < 6; i ++){
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTxType.WRITE)).get();
        }

        for(int i = 0; i < 2; i ++){
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTxType.BATCH)).get();
        }

        if(graph instanceof GraknTinkerGraph){
            assertEquals(1, openTransactions(graph));
            assertEquals(1, openTransactions(batchGraph));
        } else {
            assertEquals(6, openTransactions(graph));
            assertEquals(3, openTransactions(batchGraph));
        }
    }
    private int openTransactions(GraknGraph graph){
        if(graph == null) return 0;
        return ((AbstractGraknGraph) graph).numOpenTx();
    }

    @Test
    public void closeGraphWhenOnlyOneTransactionIsOpen(){
        assumeFalse(usingTinker()); //Tinker does not have any connections to close

        GraknSession factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        factory.close();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Graph has been closed");

        graph.putEntityType("A Thing");
    }
}
