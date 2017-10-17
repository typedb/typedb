package ai.grakn.engine;

import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.EngineID;
import ai.grakn.test.GraknTestSetup;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import spark.Service;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.GraknEngineConfig.QUEUE_CONSUMERS;
import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.REDIS_POOL_SIZE;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SENTINEL_HOST;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SENTINEL_MASTER;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * <p>
 * This class instantiates an engine server-side component once, statically until the JVM exists.
 * Any state cleanup needs to be done by tests themselves - more code, more work, but more performant
 * test suite. So all unit tests will share the same components and will be written with that in mind.  
 * </p>
 * 
 * @author borislav
 *
 */
public class EngineTestHelper {
    
    private static volatile GraknEngineConfig config = null;
    
    public static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
    }
    
    /**
     * Create (once only, statically) the GraknEngineConfig as per configuration file and return it.
     */
    public static synchronized GraknEngineConfig config() {
        if (config != null) {
            return config;
        }
        config = GraknEngineConfig.create();
        config.setConfigProperty(GraknEngineConfig.SERVER_PORT_NUMBER, String.valueOf(findAvailablePort()));
        return config;
    }
    
    private static volatile GraknEngineServer server = null;
    
    /**
     * <p>
     * Creates and initializes all components for running a self-contained, embedded engine server.
     * </p>
     * <p>
     * Make sure it's invoked before any test that needs to access engine components. It's an 
     * idempotent operation - it can be invoked many times without repercussions.
     * </p> 
     */
    public static synchronized void engine() {
        if (server != null) {
            return;
        }
        server = EngineTestHelper.graknEngineServer(config());
        server.start();
    }

    /**
     * Similarly to {@link EngineTestHelper#engine()} it creates a test engine with the ability to write {@link ai.grakn.GraknTx} to a
     * persistent backend if needed
     */
    public static synchronized void engineWithKBs() {
        GraknTestSetup.startCassandraIfNeeded();
        engine();
    }

    /**
     * Shutdown the engine server.
     */
    public static synchronized void noEngine() {
        if (server == null) {
            return;
        }
        try {
            server.close();
        }
        finally {
            server = null;
        }
    }
    
    public static synchronized GraknEngineServer graknEngineServer(GraknEngineConfig config) {
        RedisWrapper redisWrapper = redisWrapper(config);
        return EngineTestHelper.graknEngineServer(config,redisWrapper);
    }

    private static RedisWrapper redisWrapper(GraknEngineConfig config) {
        List<String> redisUrl = GraknEngineConfig.parseCSValue(config.tryProperty(REDIS_HOST).orElse("localhost:6379"));
        List<String> sentinelUrl = GraknEngineConfig.parseCSValue(config.tryProperty(REDIS_SENTINEL_HOST).orElse(""));
        int poolSize = config.tryIntProperty(REDIS_POOL_SIZE, 32);
        boolean useSentinel = !sentinelUrl.isEmpty();
        RedisWrapper.Builder builder = RedisWrapper.builder()
                .setUseSentinel(useSentinel)
                .setPoolSize(poolSize)
                .setURI((useSentinel ? sentinelUrl : redisUrl));
        if (useSentinel) {
            builder.setMasterName(config.tryProperty(REDIS_SENTINEL_MASTER).orElse("graknmaster"));
        }
        return builder.build();
    }

    public static synchronized GraknEngineServer graknEngineServer(GraknEngineConfig config, RedisWrapper redisWrapper) {
        EngineGraknTxFactory factory = EngineGraknTxFactory.create(config.getProperties());
        Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
        LockProvider lockProvider = new JedisLockProvider(jedisPool);
        EngineID engineId = EngineID.me();
        MetricRegistry metricRegistry = new MetricRegistry();
        TaskManager result = taskManager(config, factory, jedisPool, lockProvider, engineId, metricRegistry);
        TaskManager taskManager = result;
        GraknEngineStatus graknEngineStatus = new GraknEngineStatus();
        ExecutorService executorService = TasksController.taskExecutor();
        HttpHandler httpHandler = new HttpHandler(config, Service.ignite(), factory, metricRegistry, graknEngineStatus, taskManager, executorService);
        return new GraknEngineServer(config, taskManager, factory, lockProvider, graknEngineStatus, redisWrapper, executorService, httpHandler);
    }

    private static TaskManager taskManager(GraknEngineConfig config, EngineGraknTxFactory factory, Pool<Jedis> jedisPool, LockProvider lockProvider, EngineID engineId, MetricRegistry metricRegistry) {
        TaskManager result;
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "idle"), (Gauge<Integer>) jedisPool::getNumIdle);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "active"), (Gauge<Integer>) jedisPool::getNumActive);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "waiters"), (Gauge<Integer>) jedisPool::getNumWaiters);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "max"), (Gauge<Long>) jedisPool::getMaxBorrowWaitTimeMillis);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "mean"), (Gauge<Long>) jedisPool::getMeanBorrowWaitTimeMillis);

        metricRegistry.register(name(GraknEngineServer.class, "System", "gc"), new GarbageCollectorMetricSet());
        metricRegistry.register(name(GraknEngineServer.class, "System", "threads"), new CachedThreadStatesGaugeSet(15, TimeUnit.SECONDS));
        metricRegistry.register(name(GraknEngineServer.class, "System", "memory"), new MemoryUsageGaugeSet());

        Optional<String> consumers = config.tryProperty(QUEUE_CONSUMERS);
        if (consumers.isPresent()) {
            Integer threads = Integer.parseInt(consumers.get());
            result = new RedisTaskManager(engineId, config, jedisPool, threads, factory, lockProvider, metricRegistry);
        } else {
            result = new RedisTaskManager(engineId, config, jedisPool, factory, lockProvider, metricRegistry);
        }
        return result;
    }
}
