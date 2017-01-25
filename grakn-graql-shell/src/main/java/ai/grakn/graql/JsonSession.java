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
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.net.URI;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static java.util.Spliterator.IMMUTABLE;

/**
 * Websocket session for sending and receiving JSON
 */
@WebSocket
public class JsonSession {

    private final Session session;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final BlockingQueue<Json> messages = new LinkedBlockingQueue<>();

    JsonSession(GraqlClient client, URI uri) {
        try {
            this.session = client.connect(this, uri).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    boolean isOpen() {
        return session.isOpen();
    }

    /**
     * Gets all messages from the server until a special "end" message
     * @return a stream of messages
     */
    Stream<Json> getMessagesUntilEnd() {

        // Create a spliterator that returns JSON messages until an end message is received
        Spliterator<Json> spliterator = new Spliterators.AbstractSpliterator<Json>(Long.MAX_VALUE, IMMUTABLE) {
            @Override
            public boolean tryAdvance(Consumer<? super Json> action) {
                Json message = getMessage();
                if (message.is(ACTION, ACTION_END)) {
                    return false;
                } else {
                    action.accept(message);
                    return true;
                }
            }
        };

        return StreamSupport.stream(spliterator, false);
    }

    private Json getMessage() {
        try {
            return messages.poll(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void sendJson(Json json) throws WebSocketException {
        try {
            executor.submit(() -> {
                try {
                    session.getRemote().sendString(json.toString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) throws IOException, ExecutionException, InterruptedException {
        // 1000 = Normal close, 1001 = Going away
        if (statusCode != 1000 && statusCode != 1001) {
            System.err.println("Websocket closed, code: " + statusCode + ", reason: " + reason);
        }

        // Add dummy end message from server
        messages.add(Json.object(ACTION, ACTION_END));
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        Json json = Json.read(msg);
        messages.add(json);
    }
}
