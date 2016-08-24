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

package io.mindmaps.graql;

import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.mindmaps.constants.RESTUtil.RemoteShell.*;

@WebSocket
public class GraqlWebSocket {

    private final String namespace;
    private final CompletableFuture<Session> session = new CompletableFuture<>();
    private AtomicInteger queryId = new AtomicInteger(0);

    private final ConcurrentMap<Integer, BlockingQueue<Optional<String>>> queryResults = new ConcurrentHashMap<>();

    public GraqlWebSocket(String namespace) {
        this.namespace = namespace;
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws IOException, ExecutionException, InterruptedException {
        this.session.complete(session);
        sendJson(Json.object(ACTION, ACTION_NAMESPACE, NAMESPACE, namespace));
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        System.out.println("RECEIVED: " + msg);

        Json json = Json.read(msg);

        if (json.is(ACTION, ACTION_QUERY)) {
            int id = json.at(QUERY_ID).asInteger();
            boolean end = json.at(QUERY_END, false).asBoolean();

            BlockingQueue<Optional<String>> queue = queryResults.get(id);

            if (!end) {
                List<Json> lines = json.at(QUERY_LINES).asJsonList();
                lines.forEach(line -> queue.offer(Optional.of(line.asString())));
            } else {
                // Add empty to indicate end of results
                queue.offer(Optional.empty());
                queryResults.remove(id);
            }
        }
    }

    public Stream<String> sendQuery(String query) throws IOException, ExecutionException, InterruptedException {
        int id = queryId.getAndIncrement();

        BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();
        queryResults.put(id, queue);

        sendJson(Json.object(
                ACTION, ACTION_QUERY,
                QUERY_ID, id,
                QUERY, query
        ));

        return BlockingQueueStream.streamFromBlockingQueue(queue);
    }

    public void commit() throws IOException, ExecutionException, InterruptedException {
        sendJson(Json.object(ACTION, ACTION_COMMIT));
    }

    private void sendJson(Json json) throws IOException, ExecutionException, InterruptedException {
        session.get().getRemote().sendString(json.toString());
    }
}
