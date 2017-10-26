/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import spark.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.grakn.GraknConfigKey.REDIS_HOST;
import static ai.grakn.GraknConfigKey.REDIS_POOL_SIZE;
import static ai.grakn.GraknConfigKey.REDIS_SENTINEL_HOST;
import static ai.grakn.GraknConfigKey.REDIS_SENTINEL_MASTER;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * Static configurator for classes
 *
 * @author Michele Orsi
 */
public class GraknCreator {

    private final static EngineID ENGINE_ID = engineId();
    private final static Service SPARK_SERVICE = sparkService();
    private final static GraknEngineStatus GRAKN_ENGINE_STATUS = graknEngineStatus();
    private final static MetricRegistry METRIC_REGISTRY = metricRegistry();
    private final static ExecutorService EXECUTOR_SERVICE = executorService();
    private final static GraknEngineConfig GRAKN_ENGINE_CONFIG = graknEngineConfig();

    private static GraknEngineServer graknEngineServer;
    private static RedisWrapper redisWrapper;
    private static LockProvider lockProvider;
    private static EngineGraknTxFactory engineGraknTxFactory;
    private static TaskManager taskManager;

    protected static EngineID engineId() {
        return EngineID.me();
    }
    protected static Service sparkService() {
        return Service.ignite();
    }
    protected static GraknEngineStatus graknEngineStatus() {
        return new GraknEngineStatus();
    }
    protected static MetricRegistry metricRegistry() { return new MetricRegistry(); }
    protected static ExecutorService executorService() {
        return TasksController.taskExecutor();
    }
    protected static GraknEngineConfig graknEngineConfig() {
        return GraknEngineConfig.create();
    }

    static synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime) {
        if (graknEngineServer == null) {
            EngineGraknTxFactory factory = instantiateGraknTxFactory(GRAKN_ENGINE_CONFIG);
            RedisWrapper redisWrapper = instantiateRedis(GRAKN_ENGINE_CONFIG);
            Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
            LockProvider lockProvider = instantiateLock(jedisPool);
            TaskManager taskManager = instantiateTaskManager(METRIC_REGISTRY, GRAKN_ENGINE_CONFIG, ENGINE_ID, factory, jedisPool, lockProvider);
            HttpHandler httpHandler = new HttpHandler(GRAKN_ENGINE_CONFIG, SPARK_SERVICE, factory, METRIC_REGISTRY, GRAKN_ENGINE_STATUS, taskManager, EXECUTOR_SERVICE);
            graknEngineServer = new GraknEngineServer(GRAKN_ENGINE_CONFIG, taskManager, factory, lockProvider, GRAKN_ENGINE_STATUS, redisWrapper, EXECUTOR_SERVICE, httpHandler, ENGINE_ID);
            Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
            runtime.addShutdownHook(thread);
        }
        return graknEngineServer;
    }

    private static synchronized RedisWrapper instantiateRedis(GraknEngineConfig config) {
        if (redisWrapper == null) {
            redisWrapper = redisWrapper(config);
        }
        return redisWrapper;
    }

    protected static RedisWrapper redisWrapper(GraknEngineConfig config) {
        List<String> redisUrl = config.getProperty(REDIS_HOST);
        List<String> sentinelUrl = config.getProperty(REDIS_SENTINEL_HOST);
        int poolSize = config.getProperty(REDIS_POOL_SIZE);
        boolean useSentinel = !sentinelUrl.isEmpty();
        RedisWrapper.Builder builder = RedisWrapper.builder()
                .setUseSentinel(useSentinel)
                .setPoolSize(poolSize)
                .setURI((useSentinel ? sentinelUrl : redisUrl));
        if (useSentinel) {
            builder.setMasterName(config.getProperty(REDIS_SENTINEL_MASTER));
        }
        return builder.build();
    }

    private static synchronized LockProvider instantiateLock(Pool<Jedis> jedisPool) {
        if (lockProvider == null) {
            lockProvider = lockProvider(jedisPool);
        }
        return lockProvider;
    }

    protected static JedisLockProvider lockProvider(Pool<Jedis> jedisPool) {
        return new JedisLockProvider(jedisPool);
    }

    static synchronized EngineGraknTxFactory instantiateGraknTxFactory(GraknEngineConfig config) {
        if (engineGraknTxFactory == null) {
            engineGraknTxFactory = engineGraknTxFactory(config);
        }
        return engineGraknTxFactory;
    }

    protected static EngineGraknTxFactory engineGraknTxFactory(GraknEngineConfig config) {
        return EngineGraknTxFactory.create(config.getProperties());
    }

    /**
     * Check in with the properties file to decide which type of task manager should be started
     * and return the TaskManager
     *
     * @param jedisPool
     */
    private static synchronized TaskManager instantiateTaskManager(MetricRegistry metricRegistry, GraknEngineConfig config, EngineID engineId, EngineGraknTxFactory factory,
                                                           final Pool<Jedis> jedisPool,
                                                           final LockProvider lockProvider) {
        if (taskManager == null) {
            taskManager = taskManager(config, factory, jedisPool, lockProvider, engineId, metricRegistry);
        }
        return taskManager;
    }

    protected static TaskManager taskManager(GraknEngineConfig config, EngineGraknTxFactory factory, Pool<Jedis> jedisPool, LockProvider lockProvider, EngineID engineId, MetricRegistry metricRegistry) {
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "idle"), (Gauge<Integer>) jedisPool::getNumIdle);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "active"), (Gauge<Integer>) jedisPool::getNumActive);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "waiters"), (Gauge<Integer>) jedisPool::getNumWaiters);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "max"), (Gauge<Long>) jedisPool::getMaxBorrowWaitTimeMillis);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "mean"), (Gauge<Long>) jedisPool::getMeanBorrowWaitTimeMillis);

        metricRegistry.register(name(GraknEngineServer.class, "System", "gc"), new GarbageCollectorMetricSet());
        metricRegistry.register(name(GraknEngineServer.class, "System", "threads"), new CachedThreadStatesGaugeSet(15, TimeUnit.SECONDS));
        metricRegistry.register(name(GraknEngineServer.class, "System", "memory"), new MemoryUsageGaugeSet());

        int consumers = config.getProperty(GraknConfigKey.QUEUE_CONSUMERS);
        return new RedisTaskManager(engineId, config, jedisPool, consumers, factory, lockProvider, metricRegistry);
    }


}

