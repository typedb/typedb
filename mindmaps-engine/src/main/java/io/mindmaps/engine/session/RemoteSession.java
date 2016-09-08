/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.session;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.factory.GraphFactory;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.mindmaps.util.REST.RemoteShell.ACTION;
import static io.mindmaps.util.REST.RemoteShell.ACTION_AUTOCOMPLETE;
import static io.mindmaps.util.REST.RemoteShell.ACTION_COMMIT;
import static io.mindmaps.util.REST.RemoteShell.ACTION_NAMESPACE;
import static io.mindmaps.util.REST.RemoteShell.ACTION_QUERY;
import static io.mindmaps.util.REST.RemoteShell.ACTION_QUERY_END;
import static io.mindmaps.util.REST.RemoteShell.NAMESPACE;

/**
 * Web socket for running a Graql shell
 */
@WebSocket
public class RemoteSession {
    private final Map<Session, GraqlSession> sessions = new HashMap<>();
    private final Function<String, MindmapsGraph> getGraph;

    // This constructor is magically invoked by spark's websocket stuff
    @SuppressWarnings("unused")
    public RemoteSession() {
        this(GraphFactory.getInstance()::getGraph);
    }

    public RemoteSession(Function<String, MindmapsGraph> getGraph) {
        this.getGraph = getGraph;
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        sessions.remove(session).close();
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
        try {
            Json json = Json.read(message);

            switch (json.at(ACTION).asString()) {
                case ACTION_NAMESPACE:
                    startSession(session, json);
                    break;
                case ACTION_QUERY:
                    sessions.get(session).executeQuery(json);
                    break;
                case ACTION_QUERY_END:
                    sessions.get(session).stopQuery();
                    break;
                case ACTION_COMMIT:
                    sessions.get(session).commit();
                    break;
                case ACTION_AUTOCOMPLETE:
                    sessions.get(session).autocomplete(json);
                    break;
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Start a new Graql shell session
     */
    private void startSession(Session session, Json json) {
        String namespace = json.at(NAMESPACE).asString();
        MindmapsGraph graph = getGraph.apply(namespace);
        GraqlSession graqlSession = new GraqlSession(session, graph);
        sessions.put(session, graqlSession);
    }
}

