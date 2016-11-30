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

package ai.grakn.engine.session;

import ai.grakn.GraknGraph;
import ai.grakn.factory.GraphFactory;
import ai.grakn.util.REST;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Web socket for running a Graql shell
 */
@WebSocket
public class RemoteSession {
    private final Map<Session, GraqlSession> sessions = new HashMap<>();
    private final Function<String, GraknGraph> getGraph;
    private final Logger LOG = LoggerFactory.getLogger(RemoteSession.class);

    // This constructor is magically invoked by spark's websocket stuff
    @SuppressWarnings("unused")
    public RemoteSession() {
        this(GraphFactory.getInstance()::getGraph);
    }

    public RemoteSession(Function<String, GraknGraph> getGraph) {
        this.getGraph = getGraph;
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        String message = "Websocket closed, code: " + statusCode + ", reason: " + reason;
        // 1000 = Normal close, 1001 = Going away
        if (statusCode == 1000 || statusCode == 1001) {
            LOG.debug(message);
        } else {
            LOG.error(message);
        }
        sessions.remove(session).close();
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
        try {
            Json json = Json.read(message);

            switch (json.at(REST.RemoteShell.ACTION).asString()) {
                case REST.RemoteShell.ACTION_INIT:
                    startSession(session, json);
                    break;
                case REST.RemoteShell.ACTION_QUERY:
                    sessions.get(session).receiveQuery(json);
                    break;
                case REST.RemoteShell.ACTION_END:
                    sessions.get(session).executeQuery();
                    break;
                case REST.RemoteShell.ACTION_QUERY_ABORT:
                    sessions.get(session).abortQuery();
                    break;
                case REST.RemoteShell.ACTION_COMMIT:
                    sessions.get(session).commit();
                    break;
                case REST.RemoteShell.ACTION_ROLLBACK:
                    sessions.get(session).rollback();
                    break;
                case REST.RemoteShell.ACTION_DISPLAY:
                    sessions.get(session).setDisplayOptions(json);
                    break;
            }
        } catch (Throwable e) {
            LOG.error("Exception",e);
            throw e;
        }
    }

    /**
     * Start a new Graql shell session
     */
    private void startSession(Session session, Json json) {
        String keyspace = json.at(REST.RemoteShell.KEYSPACE).asString();
        String outputFormat = json.at(REST.RemoteShell.OUTPUT_FORMAT).asString();
        GraqlSession graqlSession = new GraqlSession(session, () -> getGraph.apply(keyspace), outputFormat);
        sessions.put(session, graqlSession);
    }
}

