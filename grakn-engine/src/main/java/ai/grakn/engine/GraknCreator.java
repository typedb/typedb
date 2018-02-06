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
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.GrpcServer;
import ai.grakn.engine.task.BackgroundTaskRunner;
import ai.grakn.engine.task.postprocessing.CountPostProcessor;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.PostProcessingTask;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.RedisIndexStorage;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;
import spark.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Static configurator for classes
 *
 * @author Michele Orsi
 */
public class GraknCreator {

    private final EngineID engineID;
    private final Service sparkService;
    private final GraknEngineStatus graknEngineStatus;
    private final MetricRegistry metricRegistry;
    private final GraknConfig graknEngineConfig;

    private GraknEngineServer graknEngineServer;
    private RedisWrapper redisWrapper;
    private LockProvider lockProvider;
    private EngineGraknTxFactory engineGraknTxFactory;

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

    private static EngineID engineId() {
        return EngineID.me();
    }

    private static Service sparkService() {
        return Service.ignite();
    }

    private static GraknEngineStatus newGraknEngineStatus() {
        return new GraknEngineStatus();
    }

    protected static MetricRegistry newMetricRegistry() {
        return new MetricRegistry();
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public static GraknCreator create(
            EngineID id, Service spark, GraknEngineStatus status, MetricRegistry metricRegistry, GraknConfig config,
            RedisWrapper redisWrapper
    ) {
        return new GraknCreator(id, spark, status, metricRegistry, config, redisWrapper);
    }

    public static GraknCreator create() {
        GraknConfig config = GraknConfig.create();
        return create(engineId(), sparkService(), newGraknEngineStatus(), newMetricRegistry(), config, RedisWrapper.create(config));
    }

    public synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime, Collection<HttpController> collaborators) throws IOException {
        if (graknEngineServer == null) {
            Pool<Jedis> jedisPool = redisWrapper.getJedisPool();
            LockProvider lockProvider = instantiateLock(jedisPool);
            EngineGraknTxFactory factory = instantiateGraknTxFactory(graknEngineConfig, lockProvider);
            IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, RedisIndexStorage.create(jedisPool, metricRegistry));
            CountPostProcessor countPostProcessor = CountPostProcessor.create(graknEngineConfig, factory, lockProvider, metricRegistry, RedisCountStorage.create(jedisPool, metricRegistry));
            PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
            int grpcPort = graknEngineConfig.getProperty(GraknConfigKey.GRPC_PORT);
            GrpcServer grpcServer = GrpcServer.create(grpcPort, engineGraknTxFactory);
            HttpHandler httpHandler = new HttpHandler(graknEngineConfig, sparkService, factory, metricRegistry, graknEngineStatus, postProcessor, grpcServer, collaborators);
            BackgroundTaskRunner taskRunner = configureBackgroundTaskRunner(factory, indexPostProcessor);
            graknEngineServer = new GraknEngineServer(graknEngineConfig, factory, lockProvider, graknEngineStatus, redisWrapper, httpHandler, engineID, taskRunner);
            Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
            runtime.addShutdownHook(thread);
        }
        return graknEngineServer;
    }

    public synchronized GraknEngineServer instantiateGraknEngineServer(Runtime runtime) throws IOException {
        return instantiateGraknEngineServer(runtime, Collections.emptyList());
    }

        private BackgroundTaskRunner configureBackgroundTaskRunner(EngineGraknTxFactory factory, IndexPostProcessor postProcessor) {
        PostProcessingTask postProcessingTask = new PostProcessingTask(factory, postProcessor, graknEngineConfig);
        BackgroundTaskRunner taskRunner = new BackgroundTaskRunner(graknEngineConfig);
        taskRunner.register(postProcessingTask);
        return taskRunner;
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

}