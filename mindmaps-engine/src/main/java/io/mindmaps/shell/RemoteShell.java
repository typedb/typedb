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

package io.mindmaps.shell;

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import mjson.Json;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.mindmaps.constants.RESTUtil.RemoteShell.*;

@WebSocket
public class RemoteShell {

    private final Map<Session, MindmapsGraph> graphs = new HashMap<>();

    @OnWebSocketConnect
    public void onConnect(Session session) {
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        graphs.get(session).close();
        graphs.remove(session);
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
        System.out.println("RECEIVED: " + message);

        Json json = Json.read(message);

        switch (json.at(ACTION).asString()) {
            case ACTION_NAMESPACE:
                setNamespace(session, json.at(NAMESPACE).asString());
                break;
            case ACTION_QUERY:
                executeQuery(session, json);
                break;
        }
    }

    private void setNamespace(Session session, String namespace) {
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(namespace);
        graphs.put(session, graph);
    }

    private void executeQuery(Session session, Json json) {
        int queryId = json.at(QUERY_ID).asInteger();

        MindmapsTransaction transaction = graphs.get(session).getTransaction();
        QueryParser parser = QueryParser.create(transaction);
        String queryString = json.at(QUERY).asString();

        RemoteEndpoint remote = session.getRemote();

        Stream<String> results = parser.parseMatchQuery(queryString).resultsString();

        results.forEach(result -> sendQueryResult(remote, queryId, result));
        sendQueryEnd(remote, queryId);
    }

    private void sendQueryResult(RemoteEndpoint remote, int queryId, String result) {
        Json response = Json.object(
                ACTION, ACTION_QUERY,
                QUERY_ID, queryId,
                QUERY_LINES, Json.array(result)
        );

        try {
            remote.sendString(response.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendQueryEnd(RemoteEndpoint remote, int queryId) {
        Json response = Json.object(
                ACTION, ACTION_QUERY,
                QUERY_ID, queryId,
                QUERY_END, true
        );

        try {
            remote.sendString(response.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
