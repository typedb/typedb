package ai.grakn.engine.tasks.manager.redisqueue;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.TaskId;
import ai.grakn.engine.TaskStatus;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.GenericLockProvider;
import ai.grakn.engine.tasks.manager.TaskConfiguration;
import ai.grakn.engine.tasks.manager.TaskSchedule;
import ai.grakn.engine.tasks.manager.TaskState;
import ai.grakn.engine.tasks.manager.TaskState.Priority;
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask;
import ai.grakn.engine.util.EngineID;
import ai.grakn.util.EmbeddedRedis;
import static ai.grakn.util.REST.Request.COMMIT_LOG_COUNTING;
import static ai.grakn.util.REST.Request.KEYSPACE;
import com.codahale.metrics.MetricRegistry;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import mjson.Json;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTaskManagerTest {

    private static final int PORT = 9899;
    private static final int MAX_TOTAL = 128;
    public static final GraknEngineConfig CONFIG = GraknEngineConfig.create();
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final EngineID engineID = EngineID.of("engineID");
    public static final GenericLockProvider LOCK_PROVIDER = new GenericLockProvider(
            GenericLockProvider.LOCAL_LOCK_FUNCTION);

    private static JedisPool jedisPool;
    private static EngineGraknGraphFactory engineGraknGraphFactory;

    private ExecutorService executor;
    private RedisTaskManager taskManager;

    @BeforeClass
    public static void setupClass() {
        EmbeddedRedis.start(PORT);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_TOTAL);
        jedisPool = new JedisPool(poolConfig, "localhost", 9899);
        assertFalse(jedisPool.isClosed());
        engineGraknGraphFactory = EngineGraknGraphFactory.create(CONFIG.getProperties());
    }

    @AfterClass
    public static void tearDownClass() {
        jedisPool.close();
        EmbeddedRedis.stop();
    }

    @Before
    public void setUp() {
        int nThreads = 3;
        executor = Executors.newFixedThreadPool(nThreads);
        taskManager = new RedisTaskManager(engineID, CONFIG, jedisPool, nThreads, engineGraknGraphFactory, LOCK_PROVIDER, metricRegistry);
        taskManager.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        taskManager.close();
        executor.awaitTermination(3, TimeUnit.SECONDS);
    }

    @Test
    public void testBasicBehaviour() throws ExecutionException, RetryException {
        TaskId generate = TaskId.generate();
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskManagerTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        taskManager.addTask(state, testConfig(generate));
        Retryer<Boolean> retryStrategy = RetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterAttempt(10))
                .retryIfResult(aBoolean -> false)
                .retryIfExceptionOfType(ai.grakn.exception.GraknBackendException.class)
                .withWaitStrategy(WaitStrategies.exponentialWait(10, 3, TimeUnit.SECONDS))
                .build();
        retryStrategy.call(() -> taskManager.storage().getState(state.getId()) != null);
        assertEquals(TaskStatus.COMPLETED, taskManager.storage().getState(state.getId()).status());
    }

    private TaskConfiguration testConfig(TaskId generate) {
        return TaskConfiguration.of(Json.object(
                KEYSPACE, "keyspace",
                COMMIT_LOG_COUNTING, 3,
                "id", generate.getValue()
        ));
    }
}