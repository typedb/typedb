/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.data.QueueSanityCheck;
import ai.grakn.engine.data.RedisSanityCheck;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.rpc.KeyspaceService;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.engine.lock.JedisLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.rpc.ServerOpenRequest;
import ai.grakn.engine.rpc.SessionService;
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
import ai.grakn.engine.rpc.OpenRequest;
import brave.Tracing;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;
import brave.grpc.GrpcTracing;
import com.codahale.metrics.MetricRegistry;
import io.grpc.ServerBuilder;

import java.util.Collection;
import java.util.Collections;

/**
 * This is a factory class which contains methods for instantiating a {@link Server} in different ways.
 *
 * @author Michele Orsi
 */
public class ServerFactory {
    /**
     * Create a {@link Server} configured for Grakn Core. Grakn Queue (which is needed for post-processing and distributed locks) is implemented with Redis as the backend store
     *
     * @return a {@link Server} instance configured for Grakn Core
     */
    public static Server createServer() {
        // grakn engine configuration
        EngineID engineId = EngineID.me();
        GraknConfig config = GraknConfig.create();
        ServerStatus status = new ServerStatus();

        MetricRegistry metricRegistry = new MetricRegistry();

        // redis
        RedisWrapper redisWrapper = RedisWrapper.create(config);
        QueueSanityCheck queueSanityCheck = new RedisSanityCheck(redisWrapper);

        // distributed locks
        LockProvider lockProvider = new JedisLockProvider(redisWrapper.getJedisPool());


        KeyspaceStore keyspaceStore = new KeyspaceStoreImpl(config);

        // tx-factory
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);


        // post-processing
        IndexStorage indexStorage =  RedisIndexStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        CountStorage countStorage = RedisCountStorage.create(redisWrapper.getJedisPool(), metricRegistry);
        IndexPostProcessor indexPostProcessor = IndexPostProcessor.create(lockProvider, indexStorage);
        CountPostProcessor countPostProcessor = CountPostProcessor.create(config, engineGraknTxFactory, lockProvider, metricRegistry, countStorage);
        PostProcessor postProcessor = PostProcessor.create(indexPostProcessor, countPostProcessor);

        // http services: spark, http controller, and gRPC server
        spark.Service sparkHttp = spark.Service.ignite();
        Collection<HttpController> httpControllers = Collections.emptyList();
        ServerRPC rpcServerRPC = configureServerRPC(config, engineGraknTxFactory, postProcessor, keyspaceStore);

        return createServer(engineId, config, status, sparkHttp, httpControllers, rpcServerRPC, engineGraknTxFactory, metricRegistry, queueSanityCheck, lockProvider, postProcessor, keyspaceStore);
    }

    /**
     * Allows the creation of a {@link Server} instance with various configurations
     * @return a {@link Server} instance
     */

    public static Server createServer(
            EngineID engineId, GraknConfig config, ServerStatus serverStatus,
            spark.Service sparkHttp, Collection<HttpController> httpControllers, ServerRPC rpcServerRPC,
            EngineGraknTxFactory engineGraknTxFactory,
            MetricRegistry metricRegistry,
            QueueSanityCheck queueSanityCheck, LockProvider lockProvider, PostProcessor postProcessor, KeyspaceStore keyspaceStore) {

        ServerHTTP httpHandler = new ServerHTTP(config, sparkHttp, engineGraknTxFactory, metricRegistry, serverStatus, postProcessor, rpcServerRPC, httpControllers);

        BackgroundTaskRunner taskRunner = configureBackgroundTaskRunner(config, engineGraknTxFactory, postProcessor.index());

        Server server = new Server(engineId, config, serverStatus, lockProvider, queueSanityCheck, httpHandler, taskRunner, keyspaceStore);

        Thread thread = new Thread(server::close, "grakn-server-shutdown");
        Runtime.getRuntime().addShutdownHook(thread);

        return server;
    }

    private static BackgroundTaskRunner configureBackgroundTaskRunner(GraknConfig graknEngineConfig, EngineGraknTxFactory factory, IndexPostProcessor postProcessor) {
        PostProcessingTask postProcessingTask = new PostProcessingTask(factory, postProcessor, graknEngineConfig);
        BackgroundTaskRunner taskRunner = new BackgroundTaskRunner(graknEngineConfig);
        taskRunner.register(postProcessingTask);
        return taskRunner;
    }

    private static ServerRPC configureServerRPC(GraknConfig config, EngineGraknTxFactory engineGraknTxFactory, PostProcessor postProcessor, KeyspaceStore keyspaceStore) {
        System.out.println(keyspaceStore);
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        // Brave Instrumentation
        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

        Tracing tracing = Tracing.newBuilder()
                .localServiceName("query-benchmark-server")
                .supportsJoin(false)
                .spanReporter(reporter)
                .build();

//        GrpcTracing grpcTracing = GrpcTracing.create(tracing);

        io.grpc.Server grpcServer = ServerBuilder.forPort(grpcPort)
//                .addService(ServerInterceptors.intercept(
//                        new SessionService(requestOpener, postProcessor), grpcTracing.newServerInterceptor()))
//                .addService(ServerInterceptors.intercept(
//                        new KeyspaceService(keyspaceStore), grpcTracing.newServerInterceptor()))
//                  .intercept(grpcTracing.newServerInterceptor())
                .addService(new SessionService(requestOpener, postProcessor))
                .addService(new KeyspaceService(keyspaceStore))
                .build();

        return ServerRPC.create(grpcServer);
    }

}