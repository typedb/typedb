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

package hypergraph.test.behaviour.connection.session;

import hypergraph.Hypergraph;
import hypergraph.test.behaviour.connection.ConnectionSteps;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Steps {

    @When("connection open session(s) for keyspace(s):")
    public void connection_open_sessions_for_keyspaces(List<String> names) {
        for (String name : names) {
            ConnectionSteps.sessions.add(ConnectionSteps.hypergraph.session(name));
        }
    }

    @When("connection open sessions in parallel for keyspaces:")
    public void connection_open_sessions_in_parallel_for_keyspaces(List<String> names) {
        assertTrue(ConnectionSteps.THREAD_POOL_SIZE >= names.size());

        for (String name : names) {
            ConnectionSteps.sessionsParallel.add(CompletableFuture.supplyAsync(
                    () -> ConnectionSteps.hypergraph.session(name),
                    ConnectionSteps.threadPool
            ));
        }
    }

    @Then("session(s) is/are null: {bool}")
    public void sessions_are_null(Boolean isNull) {
        for (Hypergraph.Session session : ConnectionSteps.sessions) {
            assertEquals(isNull, isNull(session));
        }
    }

    @Then("session(s) is/are open: {bool}")
    public void sessions_are_open(Boolean isOpen) {
        for (Hypergraph.Session session : ConnectionSteps.sessions) {
            assertEquals(isOpen, session.isOpen());
        }
    }

    @Then("sessions in parallel are null: {bool}")
    public void sessions_in_parallel_are_null(Boolean isNull) {
        Stream<CompletableFuture<Void>> assertions = ConnectionSteps.sessionsParallel
                .stream().map(futureSession -> futureSession.thenApplyAsync(session -> {
                    assertEquals(isNull, isNull(session));
                    return null;
                }));

        CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
    }

    @Then("sessions in parallel are open: {bool}")
    public void sessions_in_parallel_are_open(Boolean isOpen) {
        Stream<CompletableFuture<Void>> assertions = ConnectionSteps.sessionsParallel
                .stream().map(futureSession -> futureSession.thenApplyAsync(session -> {
                    assertEquals(isOpen, session.isOpen());
                    return null;
                }));

        CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
    }

    @Then("session(s) has/have keyspace(s):")
    public void sessions_have_keyspaces(List<String> names) {
        assertEquals(names.size(), ConnectionSteps.sessions.size());
        Iterator<Hypergraph.Session> sessionIter = ConnectionSteps.sessions.iterator();

        for (String name : names) {
            assertEquals(name, sessionIter.next().keyspace().name());
        }
    }

    @Then("sessions in parallel have keyspaces:")
    public void sessions_in_parallel_have_keyspaces(List<String> names) {
        assertEquals(names.size(), ConnectionSteps.sessionsParallel.size());
        Iterator<CompletableFuture<Hypergraph.Session>> futureSessionIter = ConnectionSteps.sessionsParallel.iterator();
        CompletableFuture[] assertions = new CompletableFuture[names.size()];

        int i = 0;
        for (String name : names) {
            assertions[i++] = futureSessionIter.next().thenApplyAsync(session -> {
                assertEquals(name, session.keyspace().name());
                return null;
            });
        }

        CompletableFuture.allOf(assertions).join();
    }
}
