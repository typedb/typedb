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

package grakn.core.server;

import grakn.core.util.GraknConfigKey;
import grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import grakn.core.server.factory.EngineGraknTxFactory;
import grakn.core.server.lock.LockProvider;
import grakn.core.server.lock.ProcessWideLockProvider;
import grakn.core.server.rpc.KeyspaceService;
import grakn.core.server.rpc.OpenRequest;
import grakn.core.server.rpc.ServerOpenRequest;
import grakn.core.server.rpc.SessionService;
import grakn.core.server.util.EngineID;
import grakn.core.server.keyspace.KeyspaceStore;
import grakn.core.server.keyspace.KeyspaceStoreImpl;
import grakn.core.util.GraknConfig;
import brave.Tracing;
import io.grpc.ServerBuilder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

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
    public static Server createServer(boolean benchmark) {
        // grakn engine configuration
        EngineID engineId = EngineID.me();
        GraknConfig config = GraknConfig.create();
        ServerStatus status = new ServerStatus();

        // distributed locks
        LockProvider lockProvider = new ProcessWideLockProvider();

        KeyspaceStore keyspaceStore = new KeyspaceStoreImpl(config);

        // tx-factory
        EngineGraknTxFactory engineGraknTxFactory = EngineGraknTxFactory.create(lockProvider, config, keyspaceStore);


        // post-processing
        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(config, engineGraknTxFactory);

        // http services: gRPC server
        ServerRPC rpcServerRPC = configureServerRPC(config, engineGraknTxFactory, attributeDeduplicatorDaemon, keyspaceStore, benchmark);

        return createServer(engineId, config, status, rpcServerRPC, lockProvider, attributeDeduplicatorDaemon, keyspaceStore);
    }

    /**
     * Allows the creation of a {@link Server} instance with various configurations
     * @return a {@link Server} instance
     */

    public static Server createServer(
            EngineID engineId, GraknConfig config, ServerStatus serverStatus, ServerRPC rpcServer,
            LockProvider lockProvider, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore) {

        Server server = new Server(engineId, config, serverStatus, lockProvider, rpcServer, attributeDeduplicatorDaemon, keyspaceStore);

        Thread thread = new Thread(server::close, "grakn-server-shutdown");
        Runtime.getRuntime().addShutdownHook(thread);

        return server;
    }

    private static ServerRPC configureServerRPC(GraknConfig config, EngineGraknTxFactory engineGraknTxFactory, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore, boolean benchmark){
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(engineGraknTxFactory);

        if (benchmark) {
            // Brave Instrumentation
            AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

            // set a new global tracer with reporting
            Tracing.newBuilder()
                    .localServiceName("query-benchmark-server")
                    .supportsJoin(false)
                    .spanReporter(reporter)
                    .build();

        }

        io.grpc.Server grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();

        return ServerRPC.create(grpcServer);
    }

}