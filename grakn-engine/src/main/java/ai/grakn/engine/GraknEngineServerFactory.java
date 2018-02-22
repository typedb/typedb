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
import ai.grakn.engine.task.postprocessing.CountStorage;
import ai.grakn.engine.task.postprocessing.IndexPostProcessor;
import ai.grakn.engine.task.postprocessing.IndexStorage;
import ai.grakn.engine.task.postprocessing.PostProcessingTask;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisCountStorage;
import ai.grakn.engine.task.postprocessing.redisstorage.RedisIndexStorage;
import ai.grakn.engine.util.EngineID;
import com.codahale.metrics.MetricRegistry;
import spark.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Static configurator for classes
 *
 * @author Michele Orsi
 */
public class GraknEngineServerFactory {
    private static GraknEngineServer graknEngineServer;

    public static synchronized GraknEngineServer getOrCreateGraknEngineServer() throws IOException {
        if (graknEngineServer == null) {
            return createGraknEngineServer();
        }
        return graknEngineServer;
    }

    public static GraknEngineServer createGraknEngineServer() throws IOException {
        GraknConfig config = GraknConfig.create();
        RedisWrapper redisWrapper = RedisWrapper.create(config);
        MetricRegistry metricRegistry = new MetricRegistry();
        IndexStorage indexStorage =  RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config);
        return createGraknEngineServer(EngineID.me(), Service.ignite(), new GraknEngineStatus(), metricRegistry,
                config, redisWrapper, indexStorage, countStorage, lockProvider, Runtime.getRuntime(), Collections.emptyList(), engineGraknTxFactory);
    }

    public static synchronized GraknEngineServer createGraknEngineServer(
            EngineID engineID, Service sparkService, GraknEngineStatus graknEngineStatus, MetricRegistry metricRegistry, GraknConfig graknEngineConfig,
            RedisWrapper redisWrapper, IndexStorage indexStorage, CountStorage countStorage, LockProvider lockProvider, Runtime runtime, Collection<HttpController> collaborators, EngineGraknTxFactory factory) throws IOException {

        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, indexStorage);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(graknEngineConfig, factory, lockProvider, metricRegistry, countStorage);
        PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);
        int grpcPort = graknEngineConfig.getProperty(GraknConfigKey.GRPC_PORT);
        GrpcServer grpcServer = GrpcServer.create(grpcPort, factory);
        HttpHandler httpHandler = new HttpHandler(graknEngineConfig, sparkService, factory, metricRegistry, graknEngineStatus, postProcessor, grpcServer, collaborators);
        BackgroundTaskRunner taskRunner = configureBackgroundTaskRunner(graknEngineConfig, factory, indexPostProcessor);
        graknEngineServer = new GraknEngineServer(graknEngineConfig, factory, lockProvider, graknEngineStatus, redisWrapper, httpHandler, engineID, taskRunner);
        Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
        runtime.addShutdownHook(thread);
        return graknEngineServer;
    }

    public static BackgroundTaskRunner configureBackgroundTaskRunner(GraknConfig graknEngineConfig, EngineGraknTxFactory factory, IndexPostProcessor postProcessor) {
        PostProcessingTask postProcessingTask = new PostProcessingTask(factory, postProcessor, graknEngineConfig);
        BackgroundTaskRunner taskRunner = new BackgroundTaskRunner(graknEngineConfig);
        taskRunner.register(postProcessingTask);
        return taskRunner;
    }

}