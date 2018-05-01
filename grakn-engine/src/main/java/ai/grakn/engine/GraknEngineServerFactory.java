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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.data.QueueSanityCheck;
import ai.grakn.engine.data.RedisSanityCheck;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.GrpcGraknService;
import ai.grakn.engine.rpc.GrpcOpenRequestExecutorImpl;
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
import ai.grakn.factory.SystemKeyspaceSession;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import com.codahale.metrics.MetricRegistry;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import spark.Service;

import java.util.Collection;
import java.util.Collections;

import static ai.grakn.grpc.GrpcUtil.GRPC_MAX_MESSAGE_SIZE_IN_BYTES;

/**
 * This is a factory class which contains methods for instantiating a {@link GraknEngineServer} in different ways.
 *
 * @author Michele Orsi
 */
public class GraknEngineServerFactory {
    /**
     * Create a {@link GraknEngineServer} configured for Grakn Core. Grakn Queue (which is needed for post-processing and distributed locks) is implemented with Redis as the backend store
     *
     * @return a {@link GraknEngineServer} instance configured for Grakn Core
     */
    public static GraknEngineServer createGraknEngineServer() {
        // grakn engine configuration
        EngineID engineId = EngineID.me();
        GraknConfig config = GraknConfig.create();
        GraknEngineStatus status = new GraknEngineStatus();

        MetricRegistry metricRegistry = new MetricRegistry();

        // redis
        RedisWrapper redisWrapper = RedisWrapper.create(config);
        QueueSanityCheck queueSanityCheck = new RedisSanityCheck(redisWrapper);

        // distributed locks
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());


        SystemKeyspaceSession systemKeyspaceSession = new GraknSystemKeyspaceSession(config);
        GraknKeyspaceStore graknKeyspaceStore = GraknKeyspaceStoreImpl.create(systemKeyspaceSession);

        // tx-factory
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, graknKeyspaceStore);


        // post-processing
        IndexStorage indexStorage =  RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, indexStorage);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(config, engineGraknTxFactory, lockProvider, metricRegistry, countStorage);
        PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);

        // http services: spark, http controller, and gRPC server
        Service sparkHttp = Service.ignite();
        Collection<HttpController> httpControllers = Collections.emptyList();
        GrpcServer grpcServer = configureGrpcServer(config, engineGraknTxFactory, postProcessor);

        return createGraknEngineServer(engineId, config, status, sparkHttp, httpControllers, grpcServer, engineGraknTxFactory, metricRegistry, queueSanityCheck, lockProvider, postProcessor, graknKeyspaceStore);
    }

    /**
     * Allows the creation of a {@link GraknEngineServer} instance with various configurations
     * @return a {@link GraknEngineServer} instance
     */

    public static GraknEngineServer createGraknEngineServer(
            EngineID engineId, GraknConfig config, GraknEngineStatus graknEngineStatus,
            Service sparkHttp, Collection<HttpController> httpControllers, GrpcServer grpcServer,
            EngineGraknTxFactory engineGraknTxFactory,
            MetricRegistry metricRegistry,
            QueueSanityCheck queueSanityCheck, LockProvider lockProvider, PostProcessor postProcessor, GraknKeyspaceStore graknKeyspaceStore) {

        HttpHandler httpHandler = new HttpHandler(config, sparkHttp, engineGraknTxFactory, metricRegistry, graknEngineStatus, postProcessor, grpcServer, httpControllers);

        BackgroundTaskRunner taskRunner = configureBackgroundTaskRunner(config, engineGraknTxFactory, postProcessor.index());

        GraknEngineServer graknEngineServer = new GraknEngineServer(engineId, config, graknEngineStatus, lockProvider, queueSanityCheck, httpHandler, taskRunner, graknKeyspaceStore);

        Thread thread = new Thread(graknEngineServer::close, "GraknEngineServer-shutdown");
        Runtime.getRuntime().addShutdownHook(thread);

        return graknEngineServer;
    }

    private static BackgroundTaskRunner configureBackgroundTaskRunner(GraknConfig graknEngineConfig, EngineGraknTxFactory factory, IndexPostProcessor postProcessor) {
        PostProcessingTask postProcessingTask = new PostProcessingTask(factory, postProcessor, graknEngineConfig);
        BackgroundTaskRunner taskRunner = new BackgroundTaskRunner(graknEngineConfig);
        taskRunner.register(postProcessingTask);
        return taskRunner;
    }

    private static GrpcServer configureGrpcServer(GraknConfig config, EngineGraknTxFactory engineGraknTxFactory, PostProcessor postProcessor){
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        GrpcOpenRequestExecutor requestExecutor = new GrpcOpenRequestExecutorImpl(engineGraknTxFactory);
        Server grpcServer = NettyServerBuilder.forPort(grpcPort).maxMessageSize(GRPC_MAX_MESSAGE_SIZE_IN_BYTES).addService(new GrpcGraknService(requestExecutor, postProcessor)).build();
        return GrpcServer.create(grpcServer);
    }

}