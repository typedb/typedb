package ai.grakn.test.graph;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graph.internal.GraknTinkerGraph;
import ai.grakn.test.EngineContext;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.ErrorMessage;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static ai.grakn.util.ErrorMessage.IS_ABSTRACT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class GraphTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void whenCommitting_EnsureGraphTransactionIsClosed() throws Exception {
        GraknGraph graph = engine.factoryWithNewKeyspace().open(GraknTxType.WRITE);
        String keyspace = graph.getKeyspace();
        graph.putEntityType("thingy");
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
        try(GraknGraph graphBatchLoading = Grakn.session(engine.uri(), keyspace).open(GraknTxType.WRITE)) {
            graphBatchLoading.getEntityType("thingy").addEntity();
            graphBatchLoading.commit();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
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

    @Test
    public void afterCommitting_NumberOfOpenTransactionsDecrementsOnce() {
        assumeFalse(GraknTestSetup.usingTinker()); // Tinker graph only ever has one open transaction

        GraknSession session = engine.factoryWithNewKeyspace();

        GraknGraph graph = session.open(GraknTxType.READ);

        assertEquals(1, openTransactions(graph));

        graph.commit();

        assertEquals(0, openTransactions(graph));
    }

    private int openTransactions(GraknGraph graph){
        if(graph == null) return 0;
        return ((AbstractGraknGraph) graph).numOpenTx();
    }

    @Test
    public void closeGraphWhenOnlyOneTransactionIsOpen(){
        assumeFalse(GraknTestSetup.usingTinker()); //Tinker does not have any connections to close

        GraknSession factory = engine.factoryWithNewKeyspace();
        GraknGraph graph = factory.open(GraknTxType.WRITE);
        factory.close();

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(ErrorMessage.SESSION_CLOSED.getMessage(graph.getKeyspace()));

        graph.putEntityType("A thingy");
    }

    @Test
    public void whenAddingEntitiesToAbstractTypeCreatedInDifferentTransaction_Throw(){
        assumeFalse(GraknTestSetup.usingTinker());

        String label = "An Abstract thingy";

        try(GraknSession session = Grakn.session(engine.uri(), "abstractTest")){
            try(GraknGraph graph = session.open(GraknTxType.WRITE)){
                graph.putEntityType(label).setAbstract(true);
                graph.commit();
            }
        }

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(IS_ABSTRACT.getMessage(label));

        try(GraknSession session = Grakn.session(engine.uri(), "abstractTest")){
            try(GraknGraph graph = session.open(GraknTxType.WRITE)){
                graph.getEntityType(label).addEntity();
            }
        }
    }
}
