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

import grakn.client.test.setup.GraknProperties;
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

import static java.util.Objects.isNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectionSteps {

    public static int THREAD_POOL_SIZE = 32;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static GraknClient client;
    public static List<GraknClient.Session> sessions = new ArrayList<>();
    public static List<CompletableFuture<GraknClient.Session>> sessionsParallel = new ArrayList<>();
    public static Map<GraknClient.Session, List<GraknClient.Transaction>> sessionsToTransactions = new HashMap<>();
    public static Map<GraknClient.Session, List<CompletableFuture<GraknClient.Transaction>>> sessionsToTransactionsParallel = new HashMap<>();
    public static Map<CompletableFuture<GraknClient.Session>, List<CompletableFuture<GraknClient.Transaction>>> sessionsParallelToTransactionsParallel = new HashMap<>();

    private static GraknClient connect_to_grakn_core() {
        System.out.println("Establishing Connection to Grakn Core");
        String address = System.getProperty(GraknProperties.GRAKN_ADDRESS);
        assertNotNull(address);

        System.out.println("Connection to Grakn Core established");
        return new GraknClient(address);
    }

    private static GraknClient connect_to_grakn_kgms() {
        System.out.println("Establishing Connection to Grakn");
        String address = System.getProperty(GraknProperties.GRAKN_ADDRESS);
        String username = System.getProperty(GraknProperties.GRAKN_USERNAME);
        String password = System.getProperty(GraknProperties.GRAKN_PASSWORD);
        assertNotNull(address);
        assertNotNull(username);
        assertNotNull(password);

        System.out.println("Connection to Grakn KGMS established");
        return new GraknClient(address, username, password);
    }

    private static synchronized void connect_to_grakn() {
        if (!isNull(client)) return;

        System.out.println("Connecting to Grakn ...");

        String graknType = System.getProperty(GraknProperties.GRAKN_TYPE);
        assertNotNull(graknType);

        if (graknType.equals(GraknProperties.GRAKN_CORE)) {
            client = connect_to_grakn_core();
        } else if (graknType.equals(GraknProperties.GRAKN_KGMS)) {
            client = connect_to_grakn_kgms();
        } else {
            fail("Invalid type of Grakn database: ");
        }

        assertNotNull(client);
    }

    @Given("connection has been opened")
    public void connection_has_been_opened() {
        if (isNull(client)) {
            connect_to_grakn();
        }

        assertNotNull(client);
        assertTrue(client.isOpen());
    }

    @Given("connection delete all keyspaces")
    public void connection_delete_all_keyspaces() {
        for (String keyspace : client.keyspaces().retrieve()) {
            client.keyspaces().delete(keyspace);
        }
    }

    @Given("connection does not have any keyspace")
    public void connection_does_not_have_any_keyspace() {
        assertTrue(client.keyspaces().retrieve().isEmpty());
    }

    @After
    public void close_session_and_transactions() throws ExecutionException, InterruptedException {
        System.out.println("ConnectionSteps.after");
        if (sessions != null) {
            for (GraknClient.Session session : sessions) {
                if (sessionsToTransactions.containsKey(session)) {
                    for (GraknClient.Transaction transaction : sessionsToTransactions.get(session)) {
                        transaction.close();
                    }
                    sessionsToTransactions.remove(session);
                }

                if (sessionsToTransactionsParallel.containsKey(session)) {
                    for (CompletableFuture<GraknClient.Transaction> futureTransaction : sessionsToTransactionsParallel.get(session)) {
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
            for (CompletableFuture<GraknClient.Session> futureSession : sessionsParallel) {
                if (sessionsParallelToTransactionsParallel.containsKey(futureSession)) {
                    for (CompletableFuture<GraknClient.Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
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
