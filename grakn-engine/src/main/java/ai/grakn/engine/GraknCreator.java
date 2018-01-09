/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import spark.Service;

import java.util.List;
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

    protected final EngineID engineID;
    protected final Service sparkService;
    protected final GraknEngineStatus graknEngineStatus;
    protected final MetricRegistry metricRegistry;
    protected final GraknConfig graknEngineConfig;

    protected GraknEngineServer graknEngineServer;
    protected RedisWrapper redisWrapper;
    protected LockProvider lockProvider;
    protected EngineGraknTxFactory engineGraknTxFactory;
    protected TaskManager taskManager;

    public GraknCreator() {
        engineID = engineId();
        sparkService = sparkService();
        graknEngineStatus = graknEngineStatus();
        metricRegistry = metricRegistry();
        graknEngineConfig = GraknConfig.create();
    }

    protected static EngineID engineId() {
        return EngineID.me();
    }

    protected static Service sparkService() {
        return Service.ignite();
    }

    protected static GraknEngineStatus graknEngineStatus() {
        return new GraknEngineStatus();
    }

    protected static MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

    public synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime) {
        if (graknEngineServer == null) {
            RedisWrapper redisWrapper = instantiateRedis(graknEngineConfig);
            Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
            LockProvider lockProvider = instantiateLock(jedisPool);
            EngineGraknTxFactory factory = instantiateGraknTxFactory(graknEngineConfig, lockProvider);
            PostProcessor postProcessor = postProcessor(metricRegistry, graknEngineConfig, factory, jedisPool, lockProvider);
            TaskManager taskManager = instantiateTaskManager(metricRegistry, graknEngineConfig, engineID, factory, jedisPool, postProcessor);
            HttpHandler httpHandler = new HttpHandler(graknEngineConfig, sparkService, factory, metricRegistry, graknEngineStatus, taskManager, postProcessor);
            graknEngineServer = new GraknEngineServer(graknEngineConfig, taskManager, factory, lockProvider, graknEngineStatus, redisWrapper, httpHandler, engineID);
            Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
            runtime.addShutdownHook(thread);
        }
        return graknEngineServer;
    }

    protected synchronized RedisWrapper instantiateRedis(GraknConfig config) {
        if (redisWrapper == null) {
            redisWrapper = redisWrapper(config);
        }
        return redisWrapper;
    }

    protected RedisWrapper redisWrapper(GraknConfig config) {
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

    protected synchronized LockProvider instantiateLock(Pool<Jedis> jedisPool) {
        if (lockProvider == null) {
            lockProvider = lockProvider(jedisPool);
        }
        return lockProvider;
    }

    protected JedisLockProvider lockProvider(Pool<Jedis> jedisPool) {
        return new JedisLockProvider(jedisPool);
    }

    protected synchronized EngineGraknTxFactory instantiateGraknTxFactory(GraknConfig config, LockProvider lockProvider) {
        if (engineGraknTxFactory == null) {
            engineGraknTxFactory = engineGraknTxFactory(config, lockProvider);
        }
        return engineGraknTxFactory;
    }

    protected static EngineGraknTxFactory engineGraknTxFactory(GraknConfig config, LockProvider lockProvider) {
        return EngineGraknTxFactory.create(lockProvider, config);
    }

    /**
     * Check in with the properties file to decide which type of task manager should be started
     * and return the TaskManager
     *
     * @param jedisPool
     */
    protected synchronized TaskManager instantiateTaskManager(MetricRegistry metricRegistry, GraknConfig config, EngineID engineId, EngineGraknTxFactory factory,
                                                 final Pool<Jedis> jedisPool,
                                                 PostProcessor postProcessor) {
        if (taskManager == null) {
            taskManager = taskManager(config, factory, jedisPool, engineId, metricRegistry, postProcessor);
        }
        return taskManager;
    }

    protected PostProcessor postProcessor(MetricRegistry metricRegistry, GraknConfig config, EngineGraknTxFactory factory, Pool<Jedis> jedisPool, LockProvider lockProvider){
        return PostProcessor.create(config, jedisPool, factory, lockProvider, metricRegistry);
    }

    TaskManager taskManager(GraknConfig config, EngineGraknTxFactory factory, Pool<Jedis> jedisPool, EngineID engineId, MetricRegistry metricRegistry, PostProcessor postProcessor) {
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "idle"), (Gauge<Integer>) jedisPool::getNumIdle);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "active"), (Gauge<Integer>) jedisPool::getNumActive);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "waiters"), (Gauge<Integer>) jedisPool::getNumWaiters);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "max"), (Gauge<Long>) jedisPool::getMaxBorrowWaitTimeMillis);
        metricRegistry.register(name(GraknEngineServer.class, "jedis", "borrow_wait_time_ms", "mean"), (Gauge<Long>) jedisPool::getMeanBorrowWaitTimeMillis);

        metricRegistry.register(name(GraknEngineServer.class, "System", "gc"), new GarbageCollectorMetricSet());
        metricRegistry.register(name(GraknEngineServer.class, "System", "threads"), new CachedThreadStatesGaugeSet(15, TimeUnit.SECONDS));
        metricRegistry.register(name(GraknEngineServer.class, "System", "memory"), new MemoryUsageGaugeSet());

        int consumers = config.getProperty(GraknConfigKey.QUEUE_CONSUMERS);
        return new RedisTaskManager(engineId, config, jedisPool, consumers, factory, metricRegistry, postProcessor);
    }

    @VisibleForTesting
    public synchronized GraknEngineServer cleanGraknEngineServer(GraknConfig config) {
        return cleanGraknEngineServer(config, redisWrapper(config));
    }

    @VisibleForTesting
    public synchronized GraknEngineServer cleanGraknEngineServer(GraknConfig config, RedisWrapper redisWrapper) {
        Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
        LockProvider lockProvider = lockProvider(jedisPool);
        EngineGraknTxFactory factory = engineGraknTxFactory(config, lockProvider);
        MetricRegistry metricRegistry = metricRegistry();
        EngineID engineID = engineId();
        PostProcessor postProcessor = postProcessor(metricRegistry, config, factory, jedisPool, lockProvider);
        TaskManager taskManager = taskManager(config, factory, jedisPool, engineID, metricRegistry, postProcessor);
        GraknEngineStatus graknEngineStatus = graknEngineStatus();
        HttpHandler httpHandler = new HttpHandler(config, sparkService(), factory, metricRegistry, graknEngineStatus, taskManager, postProcessor);
        return new GraknEngineServer(config, taskManager, factory, lockProvider, graknEngineStatus, redisWrapper, httpHandler, engineID);
    }
}