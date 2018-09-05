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

package ai.grakn.core.server;

import ai.grakn.GraknConfigKey;
import ai.grakn.core.server.attribute.deduplicator.AttributeDeduplicatorDaemon;
import ai.grakn.core.server.controller.HttpController;
import ai.grakn.core.server.factory.EngineGraknTxFactory;
import ai.grakn.core.server.lock.ProcessWideLockProvider;
import ai.grakn.keyspace.KeyspaceStoreImpl;
import ai.grakn.core.server.lock.LockProvider;
import ai.grakn.core.server.rpc.KeyspaceService;
import ai.grakn.core.server.rpc.ServerOpenRequest;
import ai.grakn.core.server.rpc.SessionService;
import ai.grakn.core.server.util.EngineID;
import ai.grakn.core.server.rpc.OpenRequest;
import com.codahale.metrics.MetricRegistry;
import io.grpc.ServerBuilder;
import spark.Service;

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

        // distributed locks
        LockProvider lockProvider = new ProcessWideLockProvider();

        KeyspaceStore keyspaceStore = new KeyspaceStoreImpl(config);

        // tx-factory
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);


        // post-processing
        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(config, engineGraknTxFactory);

        // http services: spark, http controller, and gRPC server
        Service sparkHttp = Service.ignite();
        Collection<HttpController> httpControllers = Collections.emptyList();
        ServerRPC rpcServerRPC = configureServerRPC(config, engineGraknTxFactory, attributeDeduplicatorDaemon, keyspaceStore);

        return createServer(engineId, config, status, sparkHttp, httpControllers, rpcServerRPC, engineGraknTxFactory, metricRegistry, lockProvider, attributeDeduplicatorDaemon, keyspaceStore);
    }

    /**
     * Allows the creation of a {@link Server} instance with various configurations
     * @return a {@link Server} instance
     */

    public static Server createServer(
            EngineID engineId, GraknConfig config, ServerStatus serverStatus,
            Service sparkHttp, Collection<HttpController> httpControllers, ServerRPC rpcServerRPC,
            EngineGraknTxFactory engineGraknTxFactory,
            MetricRegistry metricRegistry,
            LockProvider lockProvider, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore) {

        ServerHTTP httpHandler = new ServerHTTP(config, sparkHttp, engineGraknTxFactory, metricRegistry, serverStatus, attributeDeduplicatorDaemon, rpcServerRPC, httpControllers);

        Server server = new Server(engineId, config, serverStatus, lockProvider, httpHandler, attributeDeduplicatorDaemon, keyspaceStore);

        Thread thread = new Thread(server::close, "grakn-server-shutdown");
        Runtime.getRuntime().addShutdownHook(thread);

        return server;
    }

    private static ServerRPC configureServerRPC(GraknConfig config, EngineGraknTxFactory engineGraknTxFactory, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore){
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        io.grpc.Server grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();

        return ServerRPC.create(grpcServer);
    }

}