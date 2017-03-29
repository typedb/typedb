package ai.grakn.test.graph;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTransactionType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.factory.EngineGraknGraphFactory;
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
import static ai.grakn.util.ErrorMessage.TRANSACTIONS_OPEN;
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
        GraknGraph graph = engine.factoryWithNewKeyspace().open(GraknTransactionType.WRITE);
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
        try(GraknGraph graphBatchLoading = Grakn.factory(Grakn.DEFAULT_URI, keyspace).open(GraknTransactionType.WRITE)) {
            graphBatchLoading.getEntityType("thing").addEntity();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSameGraphs() throws GraknValidationException {
        String key = "mykeyspace";
        GraknGraph graph1 = Grakn.factory(Grakn.DEFAULT_URI, key).open(GraknTransactionType.WRITE);
        graph1.close();
        GraknGraph graph2 = EngineGraknGraphFactory.getInstance().getGraph(key);
        assertEquals(graph1, graph2);
        graph1.close();
        graph2.close();
    }

    @Test
    public void checkNumberOfOpenTransactionsChangesAsExpected() throws ExecutionException, InterruptedException {
        GraknSession factory = engine.factoryWithNewKeyspace();
        assertEquals(0, factory.openGraphTxs());
        assertEquals(0, factory.openGraphBatchTxs());

        factory.open(GraknTransactionType.WRITE);
        assertEquals(1, factory.openGraphTxs());
        assertEquals(0, factory.openGraphBatchTxs());

        factory.open(GraknTransactionType.BATCH);
        assertEquals(1, factory.openGraphTxs());
        assertEquals(1, factory.openGraphBatchTxs());

        int expectedValue = 1;

        for(int i = 0; i < 5; i ++){
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTransactionType.WRITE)).get();
            Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTransactionType.BATCH)).get();

            if(!usingTinker()) expectedValue++;

            assertEquals(expectedValue, factory.openGraphTxs());
            assertEquals(expectedValue, factory.openGraphBatchTxs());
        }
    }

    @Test
    public void closeGraphWhenOnlyOneTransactionIsOpen(){
        assumeFalse(usingTinker()); //Tinker does not have any connections to close

        GraknSession factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTransactionType.WRITE);
        factory.close();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Graph has been closed");

        graph.putEntityType("A Thing");
    }

    @Test
    public void attemptToCloseGraphWithOpenTransactionsThenThrowException() throws ExecutionException, InterruptedException {
        assumeFalse(usingTinker()); //Only tinker really supports transactions

        GraknSession factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTransactionType.WRITE);
        Executors.newSingleThreadExecutor().submit(() -> factory.open(GraknTransactionType.WRITE)).get();

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(TRANSACTIONS_OPEN.getMessage(graph, graph.getKeyspace(), 2));

        factory.close();
    }
}
