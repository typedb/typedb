/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.Grakn;
import grakn.core.common.parameters.Options;
import grakn.core.rocks.RocksDatabase;
import grakn.core.rocks.RocksGrakn;
import io.cucumber.java.After;
import io.cucumber.java.Before;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectionSteps {

    public static int THREAD_POOL_SIZE = 32;
    public static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static RocksGrakn grakn;
    public static Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("grakn");
    public static Path logsDir = dataDir.resolve("logs");
    public static Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logsDir);
    public static List<Grakn.Session> sessions = new ArrayList<>();
    public static List<CompletableFuture<Grakn.Session>> sessionsParallel = new ArrayList<>();
    public static Map<Grakn.Session, List<Grakn.Transaction>> sessionsToTransactions = new HashMap<>();
    public static Map<Grakn.Session, List<CompletableFuture<Grakn.Transaction>>> sessionsToTransactionsParallel = new HashMap<>();
    public static Map<CompletableFuture<Grakn.Session>, List<CompletableFuture<Grakn.Transaction>>> sessionsParallelToTransactionsParallel = new HashMap<>();

    public static Grakn.Transaction tx() {
        assertFalse("There is no open session", sessions.isEmpty());
        assertFalse("There is no open transaction", sessionsToTransactions.get(sessions.get(0)).isEmpty());
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @Given("connection has been opened")
    public void connection_has_been_opened() {
        assertNotNull(grakn);
        assertTrue(grakn.isOpen());
    }

    @Given("connection does not have any database")
    public void connection_does_not_have_any_database() {
        assertTrue(grakn.databases().all().isEmpty());
    }

    @Before
    public synchronized void before() throws IOException {
        assertNull(grakn);
        resetDirectory();
        System.out.println("Connecting to Grakn ...");
        grakn = RocksGrakn.open(options);
    }

    @After
    public synchronized void after() {
        System.out.println("ConnectionSteps.after");
        sessionsToTransactions.values().forEach(l -> l.forEach(Grakn.Transaction::close));
        sessionsToTransactions.clear();
        sessionsToTransactionsParallel.values().forEach(l -> l.forEach(c -> {
            try { c.get().close(); } catch (Exception e) { e.printStackTrace(); }
        }));
        sessionsToTransactionsParallel.clear();
        sessionsParallelToTransactionsParallel.values().forEach(l -> l.forEach(c -> {
            try { c.get().close(); } catch (Exception e) { e.printStackTrace(); }
        }));
        sessionsParallelToTransactionsParallel.clear();
        sessions.forEach(Grakn.Session::close);
        sessions.clear();
        sessionsParallel.forEach(c -> c.thenAccept(Grakn.Session::close));
        sessionsParallel.clear();
        grakn.databases().all().forEach(RocksDatabase::delete);
        grakn.close();
        assertFalse(grakn.isOpen());
        grakn = null;
    }

    private static void resetDirectory() throws IOException {
        if (Files.exists(dataDir)) {
            System.out.println("Database directory exists!");
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            System.out.println("Database directory deleted!");
        }

        Files.createDirectory(dataDir);
        System.out.println("Database Directory created: " + dataDir.toString());
    }
}
