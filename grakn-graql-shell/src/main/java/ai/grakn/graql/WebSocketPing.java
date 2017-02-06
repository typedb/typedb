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
 *
 */

package ai.grakn.graql;

import mjson.Json;
import org.eclipse.jetty.websocket.api.WebSocketException;

import java.io.EOFException;
import java.io.IOException;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_PING;

/**
 * Provides a method for pinging a JSON websocket session repeatedly
 */
class WebSocketPing {

    private static final int PING_INTERVAL = 60_000;

    static void ping(JsonSession session) {
        try {
            // This runs on a daemon thread, so it will be terminated when the JVM stops
            //noinspection InfiniteLoopStatement
            while (session.isOpen()) {
                session.sendJson(Json.object(ACTION, ACTION_PING));

                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (EOFException e) {
            // Silently exit in this case, because it is causing to tests to randomly fail
            // TODO: Figure out exactly when this happens
        } catch (WebSocketException | IOException e) {
            // Report an error if the session is still open
            if (session.isOpen()) {
                throw new RuntimeException(e);
            }
        }
    }
}
