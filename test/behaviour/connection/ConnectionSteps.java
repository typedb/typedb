/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.test.behaviour.connection;

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static grakn.core.test.behaviour.server.ServerSteps.server;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConnectionSteps {

    public static int THREAD_POOL_SIZE = 32;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static List<Session> sessions = new ArrayList<>();
    public static List<CompletableFuture<Session>> sessionsParallel = new ArrayList<>();
    public static Map<Session, List<Transaction>> sessionsToTransactions = new HashMap<>();
    public static Map<Session, List<CompletableFuture<Transaction>>> sessionsToTransactionsParallel = new HashMap<>();
    public static Map<CompletableFuture<Session>, List<CompletableFuture<Transaction>>> sessionsParallelToTransactionsParallel = new HashMap<>();

    @Given("connection has been opened")
    public void connection_has_been_opened() {
        if (isNull(server)) {
            throw new RuntimeException("Reference to GraknTestServer is null");
        }
        assertNotNull(server);
    }

    @Given("connection delete all keyspaces")
    public void connection_delete_all_keyspaces() {
        // TODO re-enable after refactoring keyspace handler
//        for (String keyspace : server.keyspaces()) {
//            server.keyspaces().delete(keyspace);
//        }
    }

    @Given("connection does not have any keyspace")
    public void connection_does_not_have_any_keyspace() {
        assertTrue(server.keyspaces().isEmpty());
    }

    @After
    public void close_session_and_transactions() throws ExecutionException, InterruptedException {
        System.out.println("ConnectionSteps.after");
        if (sessions != null) {
            for (Session session : sessions) {
                if (sessionsToTransactions.containsKey(session)) {
                    for (Transaction transaction : sessionsToTransactions.get(session)) {
                        transaction.close();
                    }
                    sessionsToTransactions.remove(session);
                }

                if (sessionsToTransactionsParallel.containsKey(session)) {
                    for (CompletableFuture<Transaction> futureTransaction : sessionsToTransactionsParallel.get(session)) {
                        futureTransaction.get().close();
                    }
                    sessionsToTransactionsParallel.remove(session);
                }

                session.close();
            }
            assertTrue(sessionsToTransactions.isEmpty());
            assertTrue(sessionsToTransactionsParallel.isEmpty());
            sessions = new ArrayList<>();
            sessionsToTransactions = new HashMap<>();
            sessionsToTransactionsParallel = new HashMap<>();
        }

        if (sessionsParallel != null) {
            for (CompletableFuture<Session> futureSession : sessionsParallel) {
                if (sessionsParallelToTransactionsParallel.containsKey(futureSession)) {
                    for (CompletableFuture<Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
                        futureTransaction.get().close();
                    }
                    sessionsParallelToTransactionsParallel.remove(futureSession);
                }
                futureSession.get().close();
            }
            assertTrue(sessionsParallelToTransactionsParallel.isEmpty());
            sessionsParallel = new ArrayList<>();
            sessionsParallelToTransactionsParallel = new HashMap<>();
        }
    }
}
