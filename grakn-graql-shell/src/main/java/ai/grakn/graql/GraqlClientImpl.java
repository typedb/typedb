/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Future;

/**
 * Default implementation of GraqlClient that connects to a websocket
 */
public class GraqlClientImpl implements GraqlClient {

    private WebSocketClient client;

    private final static long TIMEOUT = 3_600_000;

    @Override
    public Future<Session> connect(Object websocket, URI uri) {
        client = new WebSocketClient();

        client.getPolicy().setIdleTimeout(TIMEOUT);

        try {
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            return client.connect(websocket, uri);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            client.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
