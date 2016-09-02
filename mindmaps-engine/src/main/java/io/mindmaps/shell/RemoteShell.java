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
import io.mindmaps.core.implementation.exception.InvalidConceptTypeException;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Instance;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.*;
import io.mindmaps.graql.internal.parser.ANSI;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.mindmaps.constants.RESTUtil.RemoteShell.*;

@WebSocket
public class RemoteShell {
    private final Map<Session, GraqlSession> sessions = new HashMap<>();
    private final Function<String, MindmapsGraph> getGraph;

    // This constructor is magically invoked by spark's websocket stuff
    @SuppressWarnings("unused")
    public RemoteShell() {
        this(GraphFactory.getInstance()::getGraph);
    }

    public RemoteShell(Function<String, MindmapsGraph> getGraph) {
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
                    String namespace = json.at(NAMESPACE).asString();
                    MindmapsGraph graph = getGraph.apply(namespace);
                    GraqlSession graqlSession = new GraqlSession(session, graph);
                    sessions.put(session, graqlSession);
                    break;
                case ACTION_QUERY:
                    sessions.get(session).executeQuery(json);
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
}

class GraqlSession {
    private final Session session;
    private final MindmapsGraph graph;
    private final Reasoner reasoner;
    private final ExecutorService queryExecutor = Executors.newSingleThreadExecutor();

    GraqlSession(Session session, MindmapsGraph graph) {
        this.session = session;
        this.graph = graph;
        reasoner = new Reasoner(graph.getTransaction());
    }

    void close() {
        queryExecutor.submit(() -> {
            try {
                graph.getTransaction().close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void executeQuery(Json json) {
        queryExecutor.submit(() -> {

            String errorMessage = null;

            try {
                MindmapsTransaction transaction = graph.getTransaction();
                QueryParser parser = QueryParser.create(transaction);
                String queryString = json.at(QUERY).asString();

                Object query = parser.parseQuery(queryString);

                Stream<String> results = Stream.empty();

                if (query instanceof MatchQueryPrinter) {
                    results = streamMatchQuery((MatchQueryPrinter) query);
                } else if (query instanceof AskQuery) {
                    results = streamAskQuery((AskQuery) query);
                } else if (query instanceof InsertQuery) {
                    results = streamInsertQuery((InsertQuery) query);
                    reasoner.linkConceptTypes();
                } else if (query instanceof DeleteQuery) {
                    executeDeleteQuery((DeleteQuery) query);
                    reasoner.linkConceptTypes();
                } else if (query instanceof ComputeQuery) {
                    Object computeResult = ((ComputeQuery) query).execute(graph);
                    if (computeResult instanceof Map) {
                        Map<Instance, ?> map = (Map<Instance, ?>) computeResult;
                        results = map.entrySet().stream().map(e -> e.getKey().getId() + "\t" + e.getValue());
                    } else {
                        results = Stream.of(computeResult.toString());
                    }
                } else {
                    errorMessage = "Unrecognized query " + query;
                }

                results.forEach(this::sendQueryResult);
            } catch (IllegalArgumentException | IllegalStateException | InvalidConceptTypeException e) {
                errorMessage = e.getMessage();
            } catch (Throwable e) {
                errorMessage = "An unexpected error occurred";
                e.printStackTrace();
            } finally {
                if (errorMessage != null) {
                    sendQueryError(errorMessage);
                } else {
                    sendQueryEnd();
                }
            }
        });
    }

    void commit() {
        queryExecutor.submit(() -> {
            try {
                graph.getTransaction().commit();
            } catch (MindmapsValidationException e) {
                sendCommitError(e.getMessage());
            }
        });
    }

    void autocomplete(Json json) {
        queryExecutor.submit(() -> {
            String queryString = json.at(QUERY).asString();
            int cursor = json.at(AUTOCOMPLETE_CURSOR).asInteger();
            Autocomplete autocomplete = Autocomplete.create(graph.getTransaction(), queryString, cursor);
            sendAutocomplete(autocomplete);
        });
    }

    private Stream<String> streamMatchQuery(MatchQueryPrinter query) {
        // Expand match query with reasoner
        query.setMatchQuery(reasoner.expand(query.getMatchQuery()));
        return query.resultsString();
    }

    private Stream<String> streamAskQuery(AskQuery query) {
        if (query.execute()) {
            return Stream.of(ANSI.color("True", ANSI.GREEN));
        } else {
            return Stream.of(ANSI.color("False", ANSI.RED));
        }
    }

    private Stream<String> streamInsertQuery(InsertQuery query) {
        return query.stream().map(Concept::getId);
    }

    private void executeDeleteQuery(DeleteQuery query) {
        query.execute();
    }

    private void sendQueryResult(String result) {
        sendJson(Json.object(
                ACTION, ACTION_QUERY,
                QUERY_LINES, Json.array(result)
        ));
    }

    private void sendQueryEnd() {
        sendJson(Json.object(ACTION, ACTION_QUERY_END));
    }

    private void sendQueryError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_QUERY_END,
                ERROR, errorMessage
        ));
    }

    private void sendCommitError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_COMMIT,
                ERROR, errorMessage
        ));
    }

    private void sendAutocomplete(Autocomplete autocomplete) {
        sendJson(Json.object(
                ACTION, ACTION_AUTOCOMPLETE,
                AUTOCOMPLETE_CANDIDATES, autocomplete.getCandidates(),
                AUTOCOMPLETE_CURSOR, autocomplete.getCursorPosition()
        ));
    }

    private void sendJson(Json json) {
        try {
            session.getRemote().sendString(json.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
