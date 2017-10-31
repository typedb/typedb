package ai.grakn.engine.session;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.util.SimpleURI;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.MockRedisRule;
import ai.grakn.util.SampleKBLoader;
import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class EngineGraknSessionTest {

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(EngineTestHelper.config().getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static EngineGraknTxFactory graknFactory;
    
    private String factoryUri = "localhost:" + EngineTestHelper.config().getProperty(GraknConfigKey.SERVER_PORT);

    @BeforeClass
    public static void beforeClass() {
        EngineTestHelper.engineWithKBs();
        graknFactory = EngineGraknTxFactory.createAndLoadSystemSchema(EngineTestHelper.config().getProperties());
    }

    @AfterClass
    public static void afterClass() {
        EngineTestHelper.noEngine();
    }

    @Test
    public void whenFetchingGraphsOfTheSameKeyspaceFromSessionOrEngineFactory_EnsureGraphsAreTheSame(){
        String keyspace = "mykeyspace";

        GraknTx graph1 = Grakn.session(factoryUri, keyspace).open(GraknTxType.WRITE);
        graph1.close();
        GraknTx graph2 = graknFactory.tx(keyspace, GraknTxType.WRITE);

        assertEquals(graph1, graph2);
        graph2.close();
    }

    @Test
    public void testBatchLoadingGraphsInitialisedCorrectly(){
        String keyspace = "mykeyspace";
        GraknTx graph1 = graknFactory.tx(keyspace, GraknTxType.WRITE);
        graph1.close();
        GraknTx graph2 = graknFactory.tx(keyspace, GraknTxType.BATCH);

        assertFalse(graph1.admin().isBatchTx());
        assertTrue(graph2.admin().isBatchTx());

        graph1.close();
        graph2.close();
    }

    @Test
    public void closeGraphWhenOnlyOneTransactionIsOpen(){
        assumeFalse(GraknTestSetup.usingTinker()); //Tinker does not have any connections to close

        GraknSession factory = Grakn.session(factoryUri, SampleKBLoader.randomKeyspace());
        GraknTx graph = factory.open(GraknTxType.WRITE);
        factory.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(ErrorMessage.SESSION_CLOSED.getMessage(graph.getKeyspace()));

        graph.putEntityType("A thingy");
    }
}
