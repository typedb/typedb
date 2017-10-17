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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.grakn.engine.GraknEngineConfig.REDIS_HOST;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SENTINEL_HOST;
import static ai.grakn.engine.GraknEngineConfig.REDIS_POOL_SIZE;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SENTINEL_MASTER;
import static ai.grakn.engine.GraknEngineConfig.QUEUE_CONSUMERS;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * Static configurator for classes
 *
 * @author Michele Orsi
 */
public class GraknCreator {

    private final static EngineID ENGINE_ID = EngineID.me();
    private final static Service SPARK_SERVICE = Service.ignite();
    private final static GraknEngineStatus GRAKN_ENGINE_STATUS = new GraknEngineStatus();
    private final static MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private final static ExecutorService EXECUTOR_SERVICE = TasksController.taskExecutor();
    private final static GraknEngineConfig GRAKN_ENGINE_CONFIG = GraknEngineConfig.create();

    private static GraknEngineServer graknEngineServer;
    private static RedisWrapper redisWrapper;
    private static LockProvider lockProvider;
    private static EngineGraknTxFactory engineGraknTxFactory;
    private static TaskManager taskManager;

    static synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime) {
        if (graknEngineServer == null) {
            EngineGraknTxFactory factory = instantiateGraknTxFactory(GRAKN_ENGINE_CONFIG);
            RedisWrapper redisWrapper = instantiateRedis(GRAKN_ENGINE_CONFIG);
            Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
            LockProvider lockProvider = instantiateLock(jedisPool);
            TaskManager taskManager = instantiateTaskManager(METRIC_REGISTRY, GRAKN_ENGINE_CONFIG, ENGINE_ID, factory, jedisPool, lockProvider);
            HttpHandler httpHandler = new HttpHandler(GRAKN_ENGINE_CONFIG, SPARK_SERVICE, factory, METRIC_REGISTRY, GRAKN_ENGINE_STATUS, taskManager, EXECUTOR_SERVICE);
            graknEngineServer = new GraknEngineServer(GRAKN_ENGINE_CONFIG, taskManager, factory, lockProvider, GRAKN_ENGINE_STATUS, redisWrapper, EXECUTOR_SERVICE, httpHandler);
            Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
            runtime.addShutdownHook(thread);
        }
        return graknEngineServer;
    }

    static synchronized RedisWrapper instantiateRedis(GraknEngineConfig prop) {
        if (redisWrapper == null) {
            List<String> redisUrl = GraknEngineConfig.parseCSValue(prop.tryProperty(REDIS_HOST).orElse("localhost:6379"));
            List<String> sentinelUrl = GraknEngineConfig.parseCSValue(prop.tryProperty(REDIS_SENTINEL_HOST).orElse(""));
            int poolSize = prop.tryIntProperty(REDIS_POOL_SIZE, 32);
            boolean useSentinel = !sentinelUrl.isEmpty();
            RedisWrapper.Builder builder = RedisWrapper.builder()
                    .setUseSentinel(useSentinel)
                    .setPoolSize(poolSize)
                    .setURI((useSentinel ? sentinelUrl : redisUrl));
            if (useSentinel) {
                builder.setMasterName(prop.tryProperty(REDIS_SENTINEL_MASTER).orElse("graknmaster"));
            }
            redisWrapper = builder.build();
        }
        return redisWrapper;
    }

    static synchronized LockProvider instantiateLock(Pool<Jedis> jedisPool) {
        if (lockProvider == null) {
            lockProvider = new JedisLockProvider(jedisPool);
        }
        return lockProvider;
    }

    static synchronized EngineGraknTxFactory instantiateGraknTxFactory(GraknEngineConfig prop) {
        if (engineGraknTxFactory == null) {
            engineGraknTxFactory = EngineGraknTxFactory.create(prop.getProperties());
        }
        return engineGraknTxFactory;
    }

    /**
     * Check in with the properties file to decide which type of task manager should be started
     * and return the TaskManager
     *
     * @param jedisPool
     */
    static synchronized TaskManager instantiateTaskManager(MetricRegistry metricRegistry, GraknEngineConfig prop, EngineID engineId, EngineGraknTxFactory factory,
                                                           final Pool<Jedis> jedisPool,
                                                           final LockProvider lockProvider) {
        if (taskManager == null) {
            TaskManager result;
            metricRegistry.register(name(GraknEngineServer.class, "jedis", "idle"), (Gauge<Integer>) jedisPool::getNumIdle);
            metricRegistry.register(name(GraknEngineServer.class, "jedis", "active"), (Gauge<Integer>) jedisPool::getNumActive);
            metricRegistry.register(name(GraknEngineServer.class, "jedis", "waiters"), (Gauge<Integer>) jedisPool::getNumWaiters);
            metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "max"), (Gauge<Long>) jedisPool::getMaxBorrowWaitTimeMillis);
            metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "mean"), (Gauge<Long>) jedisPool::getMeanBorrowWaitTimeMillis);

            metricRegistry.register(name(GraknEngineServer.class, "System", "gc"), new GarbageCollectorMetricSet());
            metricRegistry.register(name(GraknEngineServer.class, "System", "threads"), new CachedThreadStatesGaugeSet(15, TimeUnit.SECONDS));
            metricRegistry.register(name(GraknEngineServer.class, "System", "memory"), new MemoryUsageGaugeSet());

            Optional<String> consumers = prop.tryProperty(QUEUE_CONSUMERS);
            if (consumers.isPresent()) {
                Integer threads = Integer.parseInt(consumers.get());
                result = new RedisTaskManager(engineId, prop, jedisPool, threads, factory, lockProvider, metricRegistry);
            } else {
                result = new RedisTaskManager(engineId, prop, jedisPool, factory, lockProvider, metricRegistry);
            }
            taskManager = result;
        }
        return taskManager;
    }

}

