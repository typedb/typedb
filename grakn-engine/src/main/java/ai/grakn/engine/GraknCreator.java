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

import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.postprocessing.PostProcessor;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import spark.Service;

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

    /**
     * @deprecated use {@link GraknCreator#create()}.
     */
    @Deprecated
    public GraknCreator() {
        engineID = engineId();
        sparkService = sparkService();
        graknEngineStatus = graknEngineStatus();
        metricRegistry = metricRegistry();
        graknEngineConfig = GraknConfig.create();
        redisWrapper = RedisWrapper.create(graknEngineConfig);
    }

    private GraknCreator(
            EngineID id, Service spark, GraknEngineStatus status, MetricRegistry metricRegistry, GraknConfig config,
            RedisWrapper redisWrapper
    ) {
        this.engineID = id;
        this.sparkService = spark;
        this.graknEngineStatus = status;
        this.metricRegistry = metricRegistry;
        this.graknEngineConfig = config;
        this.redisWrapper = redisWrapper;
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

    public static GraknCreator create(
            EngineID id, Service spark, GraknEngineStatus status, MetricRegistry metricRegistry, GraknConfig config,
            RedisWrapper redisWrapper
    ) {
        return new GraknCreator(id, spark, status, metricRegistry, config, redisWrapper);
    }

    public static GraknCreator create() {
        GraknConfig config = GraknConfig.create();
        return create(engineId(), sparkService(), graknEngineStatus(), metricRegistry(), config, RedisWrapper.create(config));
    }

    public synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime) {
        if (graknEngineServer == null) {
            Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
            LockProvider lockProvider = instantiateLock(jedisPool);
            EngineGraknTxFactory factory = instantiateGraknTxFactory(graknEngineConfig, lockProvider);
            PostProcessor postProcessor = postProcessor(metricRegistry, graknEngineConfig, factory, jedisPool, lockProvider);
            HttpHandler httpHandler = new HttpHandler(graknEngineConfig, sparkService, factory, metricRegistry, graknEngineStatus, taskManager, postProcessor);
            graknEngineServer = new GraknEngineServer(graknEngineConfig, taskManager, factory, lockProvider, graknEngineStatus, redisWrapper, httpHandler, engineID);
            Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
            runtime.addShutdownHook(thread);
        }
        return graknEngineServer;
    }

    /**
     * @deprecated use {@link GraknCreator#redisWrapper}
     */
    @Deprecated
    protected synchronized RedisWrapper instantiateRedis(GraknConfig config) {
        return redisWrapper;
    }

    /**
     * @deprecated use {@link RedisWrapper#create(GraknConfig)}
     */
    @Deprecated
    protected RedisWrapper redisWrapper(GraknConfig config) {
        return RedisWrapper.create(config);
    }

    protected synchronized LockProvider instantiateLock(Pool<Jedis> jedisPool) {
        if (lockProvider == null) {
            lockProvider = lockProvider(jedisPool);
        }
        return lockProvider;
    }

    protected static JedisLockProvider lockProvider(Pool<Jedis> jedisPool) {
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

    protected PostProcessor postProcessor(MetricRegistry metricRegistry, GraknConfig config, EngineGraknTxFactory factory, Pool<Jedis> jedisPool, LockProvider lockProvider){
        return PostProcessor.create(config, jedisPool, factory, lockProvider, metricRegistry);
    }

}