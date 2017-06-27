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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import mjson.Json;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTaskQueueTest {

    private static final String QUEUE_NAME = "test_queue";
    private static final int PORT = 9899;
    private static final int MAX_TOTAL = 128;
    public static final GraknEngineConfig CONFIG = GraknEngineConfig.create();
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final Config config = new ConfigBuilder().build();
    private static final EngineID engineID = EngineID.of("engineID");

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
        engineGraknGraphFactory = EngineGraknGraphFactory.create(CONFIG.getProperties(), false);
    }

    @AfterClass
    public static void tearDownClass() {
        jedisPool.close();
        EmbeddedRedis.stop();
    }

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(5);
        taskManager = new RedisTaskManager(engineID, CONFIG, jedisPool, 5,
                engineGraknGraphFactory, null, metricRegistry);
        taskManager.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        taskManager.close();
        executor.awaitTermination(3, TimeUnit.SECONDS);
    }

    @Test
    public void testBasicBehaviour() throws ExecutionException, RetryException {
        RedisTaskQueue taskQueue = new RedisTaskQueue(jedisPool, new GenericLockProvider(
                GenericLockProvider.LOCAL_LOCK_FUNCTION), metricRegistry);
        taskQueue.subscribe(taskManager, executor, engineID, CONFIG, engineGraknGraphFactory);
        TaskId generate = TaskId.generate();
        TaskState state = TaskState.of(ShortExecutionMockTask.class, RedisTaskQueueTest.class.getName(), TaskSchedule.now(), Priority.LOW);
        taskQueue.putJob(Task.builder().setTaskConfiguration(
                testConfig(generate)).setTaskState(state).build());
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

    @Test
    public void testJesque() {
        final Job job = new Job("TestAction", 1, 2.3, true, "test", Arrays.asList("inner", 4.5));
        final Client client = new ClientImpl(config);
        client.enqueue("foo", job);
        client.end();

        final Worker worker = new WorkerImpl(config,
                Collections.singletonList("foo"), new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        worker.getWorkerEventEmitter().addListener(
                (event, worker1, queue, job1, runner, result, t) -> {
                    if (runner instanceof RedisTaskQueueConsumer) {
                        countDownLatch.countDown();
                    }
                }, WorkerEvent.JOB_SUCCESS);

        final Thread workerThread = new Thread(worker);
        workerThread.start();
        worker.end(true);
        try {
            countDownLatch.await(3, TimeUnit.SECONDS);
            workerThread.join();
        } catch (Exception e) {
            fail();
        }
    }

    public static class TestAction  implements  Runnable{
        private static final Logger LOG = LoggerFactory.getLogger(TestAction.class);

        private final Integer i;
        private final Double d;
        private final Boolean b;
        private final String s;
        private final List<Object> l;

        public TestAction(final Integer i, final Double d, final Boolean b, final String s, final List<Object> l) {
            this.i = i;
            this.d = d;
            this.b = b;
            this.s = s;
            this.l = l;
        }

        public void run() {
            LOG.info("TestAction.run() {} {} {} {} {}", this.i, this.d, this.b, this.s, this.l);
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOG.error("Failed to run test action", e);
            }
        }
    }
}