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

package ai.grakn.engine.session;

import ai.grakn.MindmapsGraph;
import ai.grakn.exception.ConceptException;
import ai.grakn.graql.Autocomplete;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.Reasoner;
import com.google.common.base.Splitter;
import ai.grakn.MindmapsGraph;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.graql.Autocomplete;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.Reasoner;
import mjson.Json;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_AUTOCOMPLETE;
import static ai.grakn.util.REST.RemoteShell.ACTION_ERROR;
import static ai.grakn.util.REST.RemoteShell.ACTION_PING;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static ai.grakn.util.REST.RemoteShell.AUTOCOMPLETE_CANDIDATES;
import static ai.grakn.util.REST.RemoteShell.AUTOCOMPLETE_CURSOR;
import static ai.grakn.util.REST.RemoteShell.ERROR;
import static ai.grakn.util.REST.RemoteShell.QUERY;
import static ai.grakn.util.REST.RemoteShell.QUERY_RESULT;

/**
 * A Graql shell session for a single client, running on one graph in one thread
 */
class GraqlSession {
    private final Session session;
    private final MindmapsGraph graph;
    private final Printer printer;
    private StringBuilder queryStringBuilder = new StringBuilder();
    private final Reasoner reasoner;
    private final Logger LOG = LoggerFactory.getLogger(GraqlSession.class);

    private static final int QUERY_CHUNK_SIZE = 1000;
    private static final int PING_INTERVAL = 60_000;

    private boolean queryCancelled = false;

    // All requests are run within a single thread, so they always happen in a single thread-bound transaction
    private final ExecutorService queryExecutor = Executors.newSingleThreadExecutor();

    GraqlSession(Session session, MindmapsGraph graph, Printer printer) {
        this.session = session;
        this.graph = graph;
        this.printer = printer;
        reasoner = new Reasoner(graph);

        // Begin sending pings
        Thread thread = new Thread(this::ping);
        thread.setDaemon(true);
        thread.start();
    }

    private void ping() {
        // This runs on a daemon thread, so it will be terminated when the JVM stops
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                sendJson(Json.object(ACTION, ACTION_PING));

                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (WebSocketException e) {
                // Report an error if the session is still open
                if (session.isOpen()) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    /**
     * Close the session, which will close the transaction.
     */
    void close() {
        queryExecutor.submit(() -> {
            try {
                graph.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Receive and remember part of a query
     */
    void receiveQuery(Json json) {
        queryExecutor.submit(() -> {
            String queryString = json.at(QUERY).asString();
            queryStringBuilder.append(queryString);
        });
    }

    /**
     * Execute the Graql query described in the given JSON request
     */
    void executeQuery() {
        queryExecutor.submit(() -> {

            String errorMessage = null;
            Query<?> query = null;

            try {
                String queryString = queryStringBuilder.toString();
                queryStringBuilder = new StringBuilder();

                query = graph.graql().parse(queryString);

                if (query instanceof MatchQuery) {
                    query = reasonMatchQuery((MatchQuery) query);
                }

                // Return results unless query is cancelled
                query.resultsString(printer).forEach(result -> {
                    if (queryCancelled) return;
                    sendQueryResult(result);
                });
                queryCancelled = false;

                if (!query.isReadOnly()) {
                    reasoner.linkConceptTypes();
                }

            } catch (IllegalArgumentException | IllegalStateException | ConceptException e) {
                errorMessage = e.getMessage();
            } catch (Throwable e) {
                errorMessage = "An unexpected error occurred";
                LOG.error(errorMessage,e);
            } finally {
                if (errorMessage != null) {
                    if (query != null && !query.isReadOnly()) {
                        attemptRollback();
                    }
                    sendQueryError(errorMessage);
                } else {
                    sendEnd();
                }
            }
        });
    }

    void abortQuery() {
        queryCancelled = true;
    }

    /**
     * Commit and report any errors to the client
     */
    void commit() {
        queryExecutor.submit(() -> {
            try {
                graph.commit();
            } catch (MindmapsValidationException e) {
                sendCommitError(e.getMessage());
            }

            sendEnd();
        });
    }

    /**
     * Rollback the transaction, removing uncommitted changes
     */
    void rollback() {
        queryExecutor.submit(graph::rollback);
    }

    /**
     * Find autocomplete results and send them to the client
     */
    void autocomplete(Json json) {
        queryExecutor.submit(() -> {
            String queryString = json.at(QUERY).asString();
            int cursor = json.at(AUTOCOMPLETE_CURSOR).asInteger();
            Autocomplete autocomplete = Autocomplete.create(graph, queryString, cursor);
            sendAutocomplete(autocomplete);
        });
    }

    private void attemptRollback() {
        try {
            graph.rollback();
        } catch (UnsupportedOperationException ignored) {
        }
    }

    /**
     * Apply reasoner to match query
     */
    private MatchQuery reasonMatchQuery(MatchQuery query) {
        // Expand match query with reasoner, if there are any rules in the graph
        // TODO: Make sure reasoner still applies things such as limit, even with rules in the graph
        if (!Reasoner.getRules(graph).isEmpty()) {
            return reasoner.resolveToQuery(query);
        } else {
            return query;
        }
    }

    /**
     * Send a single query result back to the client
     */
    private void sendQueryResult(String result) {
        // Split result into chunks
        Iterable<String> splitResult = Splitter.fixedLength(QUERY_CHUNK_SIZE).split(result + "\n");

        for (String resultChunk : splitResult) {
            sendJson(Json.object(
                    ACTION, ACTION_QUERY,
                    QUERY_RESULT, resultChunk
            ));
        }
    }

    /**
     * Tell the client that there are no more query results
     */
    private void sendEnd() {
        sendJson(Json.object(ACTION, ACTION_END));
    }

    /**
     * Tell the client about an error in their query
     */
    private void sendQueryError(String errorMessage) {
        // Split error into chunks
        Iterable<String> splitError = Splitter.fixedLength(QUERY_CHUNK_SIZE).split(errorMessage + "\n");

        for (String errorChunk : splitError) {
            sendJson(Json.object(
                    ACTION, ACTION_ERROR,
                    ERROR, errorChunk
            ));
        }

        sendJson(Json.object(ACTION, ACTION_END));
    }

    /**
     * Tell the client about an error during commit
     */
    private void sendCommitError(String errorMessage) {
        sendJson(Json.object(
                ACTION, ACTION_ERROR,
                ERROR, errorMessage
        ));
    }

    /**
     * Send the given autocomplete results to the client
     */
    private void sendAutocomplete(Autocomplete autocomplete) {
        sendJson(Json.object(
                ACTION, ACTION_AUTOCOMPLETE,
                AUTOCOMPLETE_CANDIDATES, autocomplete.getCandidates(),
                AUTOCOMPLETE_CURSOR, autocomplete.getCursorPosition()
        ));
    }

    /**
     * Send the given JSON to the client
     */
    private void sendJson(Json json) {
        try {
            session.getRemote().sendString(json.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
