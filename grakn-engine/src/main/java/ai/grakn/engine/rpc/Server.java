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

package ai.grakn.engine.rpc;

import java.io.IOException;

/**
 * @author Felix Chapman
 */
public class Server implements AutoCloseable {

    private final io.grpc.Server server;

    private Server(io.grpc.Server server) {
        this.server = server;
    }


    public static Server create(io.grpc.Server server) {
        return new Server(server);
    }

    /**
     * @throws IOException if unable to bind
     */
    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

}

