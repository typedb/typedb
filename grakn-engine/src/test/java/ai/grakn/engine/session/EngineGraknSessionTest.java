package ai.grakn.engine.session;

import ai.grakn.Grakn;
import ai.grakn.GraknTx;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.util.SimpleURI;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.test.GraknTestSetup;
import ai.grakn.util.EmbeddedCassandra;
import ai.grakn.util.ErrorMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import ai.grakn.util.Redis;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class EngineGraknSessionTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static GraknEngineServer graknEngineServer;

    private static EngineGraknTxFactory graknFactory = EngineGraknTxFactory.createAndLoadSystemSchema(EngineTestHelper.config().getProperties());
    
    private String factoryUri = "localhost:" + EngineTestHelper.config().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER);

    @ClassRule
    public static RuleChain chain = RuleChain
            .outerRule(new EmbeddedCassandra())
            .around(Redis.redis(new SimpleURI(EngineTestHelper.config().getProperties().getProperty(REDIS_HOST)).getPort(), true));

    @BeforeClass
    public static void beforeClass() {
        graknEngineServer = new GraknEngineServer(EngineTestHelper.config());
        graknEngineServer.start();
        graknFactory = EngineGraknTxFactory.createAndLoadSystemSchema(EngineTestHelper.config().getProperties());
    }

    @AfterClass
    public static void afterClass() {
        graknEngineServer.close();
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

        GraknSession factory = Grakn.session(factoryUri, "RandomKeySpaceIsRandom");
        GraknTx graph = factory.open(GraknTxType.WRITE);
        factory.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(ErrorMessage.SESSION_CLOSED.getMessage(graph.getKeyspace()));

        graph.putEntityType("A thingy");
    }
}
