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
import grakn.core.server.session.SessionStore;
import grakn.core.server.util.LockManager;
import grakn.core.server.util.ServerLockManager;
import grakn.core.server.rpc.KeyspaceService;
import grakn.core.server.rpc.OpenRequest;
import grakn.core.server.rpc.ServerOpenRequest;
import grakn.core.server.rpc.SessionService;
import grakn.core.server.util.EngineID;
import grakn.core.server.keyspace.KeyspaceManager;
import grakn.core.util.GraknConfig;
import io.grpc.ServerBuilder;

import grakn.benchmark.lib.serverinstrumentation.ServerTracingInstrumentation;

/**
 * This is a factory class which contains methods for instantiating a {@link Server} in different ways.
 *
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

        // distributed locks
        LockManager lockManager = new ServerLockManager();

        KeyspaceManager keyspaceStore = new KeyspaceManager(config);

        // tx-factory
        SessionStore sessionStore = SessionStore.create(lockManager, config, keyspaceStore);


        // post-processing
        AttributeDeduplicatorDaemon attributeDeduplicatorDaemon = new AttributeDeduplicatorDaemon(config, sessionStore);

        // http services: gRPC server
        io.grpc.Server serverRPC = createServerRPC(config, sessionStore, attributeDeduplicatorDaemon, keyspaceStore, benchmark);

        return createServer(engineId, config, serverRPC, lockManager, attributeDeduplicatorDaemon, keyspaceStore);
    }

    /**
     * Allows the creation of a {@link Server} instance with various configurations
     * @return a {@link Server} instance
     */

    public static Server createServer(
            EngineID engineId, GraknConfig config, io.grpc.Server rpcServer,
            LockManager lockManager, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceManager keyspaceStore) {

        Server server = new Server(engineId, config, lockManager, rpcServer, attributeDeduplicatorDaemon, keyspaceStore);

        Thread thread = new Thread(server::close, "grakn-server-shutdown");
        Runtime.getRuntime().addShutdownHook(thread);

        return server;
    }

    private static io.grpc.Server createServerRPC(GraknConfig config, SessionStore sessionStore, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceManager keyspaceStore, boolean benchmark){
        int grpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        OpenRequest requestOpener = new ServerOpenRequest(sessionStore);

        if (benchmark) {
            ServerTracingInstrumentation.initInstrumentation("server-instrumentation");
        }

        io.grpc.Server serverRPC = ServerBuilder.forPort(grpcPort)
                .addService(new SessionService(requestOpener, attributeDeduplicatorDaemon))
                .addService(new KeyspaceService(keyspaceStore))
                .build();

        return serverRPC;
    }

}