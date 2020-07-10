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

package grakn.test.behaviour.connection;

import grakn.Grakn;
import grakn.rocks.RocksGrakn;
import grakn.rocks.RocksKeyspace;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.isNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConnectionSteps {

    public static int THREAD_POOL_SIZE = 32;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static RocksGrakn grakn;
    public static Path directory = Paths.get(System.getProperty("user.dir")).resolve("grakn");
    public static List<Grakn.Session> sessions = new ArrayList<>();
    public static List<CompletableFuture<Grakn.Session>> sessionsParallel = new ArrayList<>();
    public static Map<Grakn.Session, List<Grakn.Transaction>> sessionsToTransactions = new HashMap<>();
    public static Map<Grakn.Session, List<CompletableFuture<Grakn.Transaction>>> sessionsToTransactionsParallel = new HashMap<>();
    public static Map<CompletableFuture<Grakn.Session>, List<CompletableFuture<Grakn.Transaction>>> sessionsParallelToTransactionsParallel = new HashMap<>();

    private static void resetDirectory() throws IOException {
        if (Files.exists(directory)) {
            System.out.println("Database directory exists!");
            Files.walk(directory).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(directory);
        System.out.println("Database Directory created: " + directory.toString());
    }

    private static synchronized void connect_to_grakn() throws IOException {
        if (!isNull(grakn)) return;

        resetDirectory();
        System.out.println("Connecting to Grakn ...");
        grakn = RocksGrakn.open(directory.toString());
        assertNotNull(grakn);
    }

    public static Grakn.Transaction tx() {
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @Given("connection has been opened")
    public void connection_has_been_opened() throws IOException {
        if (isNull(grakn)) {
            connect_to_grakn();
        }

        assertNotNull(grakn);
        assertTrue(grakn.isOpen());
    }

    @Given("connection delete all keyspaces")
    public void connection_delete_all_keyspaces() {
        grakn.keyspaces().getAll().forEach(RocksKeyspace::delete);
    }

    @Given("connection does not have any keyspace")
    public void connection_does_not_have_any_keyspace() {
        assertTrue(grakn.keyspaces().getAll().isEmpty());
    }

    @After
    public void connection_close() {
        grakn.close();
        grakn = null;
        sessions.clear();
        sessionsParallel.clear();
        sessionsToTransactions.clear();
        sessionsToTransactionsParallel.clear();
        sessionsParallelToTransactionsParallel.clear();
    }
}
