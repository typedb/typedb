/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */
package grakn.core.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main class in charge to start gRPC server and initialise Grakn system keyspace.
 */
public class Server implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private final io.grpc.Server serverRPC;

    public Server(io.grpc.Server serverRPC) {
        // Lock provider
        this.serverRPC = serverRPC;
    }

    public void start() throws IOException {
        serverRPC.start();
    }

    // NOTE: this method is used by Grakn KGMS and should be kept public
    public void awaitTermination() throws InterruptedException {
        serverRPC.awaitTermination();
    }

    @Override
    public void close() {
        try {
            serverRPC.shutdown();
            serverRPC.awaitTermination();
        } catch (InterruptedException e) {
            LOG.error("Exception while closing Server:", e);
            Thread.currentThread().interrupt();
        }
    }
}
