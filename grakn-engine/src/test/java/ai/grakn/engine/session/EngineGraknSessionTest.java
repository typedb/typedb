package ai.grakn.engine.session;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.SystemKeyspaceFake;
import ai.grakn.engine.controller.SparkContext;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.test.rule.SessionContext;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.test.rule.InMemoryRedisContext;
import ai.grakn.util.SampleKBLoader;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.mock;

public class EngineGraknSessionTest {
    private static final GraknEngineConfig config = GraknEngineConfig.create();
    private static final GraknEngineStatus status = mock(GraknEngineStatus.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final SystemKeyspaceFake systemKeyspace = SystemKeyspaceFake.of();

    private static EngineGraknTxFactory graknFactory;

    @ClassRule
    public static InMemoryRedisContext inMemoryRedisContext = InMemoryRedisContext.create(new SimpleURI(Iterables.getOnlyElement(config.getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    //Needed so that Grakn.session() can return a session
    @ClassRule
    public static final SparkContext sparkContext = SparkContext.withControllers(spark -> {
        new SystemController(spark, config.getProperties(), systemKeyspace, status, metricRegistry);
    });

    //Needed to start cass depending on profile
    @ClassRule
    public static final SessionContext sessionContext = SessionContext.create();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        JedisLockProvider lockProvider = new JedisLockProvider(inMemoryRedisContext.jedisPool());
        graknFactory = EngineGraknTxFactory.createAndLoadSystemSchema(lockProvider, config.getProperties());
    }

    @Test
    public void whenFetchingGraphsOfTheSameKeyspaceFromSessionOrEngineFactory_EnsureGraphsAreTheSame(){
        String keyspace = "mykeyspace";

        GraknTx graph1 = Grakn.session(sparkContext.uri(), keyspace).open(GraknTxType.WRITE);
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
        assumeFalse(GraknTestUtil.usingTinker()); //Tinker does not have any connections to close

        GraknSession factory = Grakn.session(sparkContext.uri(), SampleKBLoader.randomKeyspace());
        GraknTx graph = factory.open(GraknTxType.WRITE);
        factory.close();

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(ErrorMessage.SESSION_CLOSED.getMessage(graph.keyspace()));

        graph.putEntityType("A thingy");
    }
}
