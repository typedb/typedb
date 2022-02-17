/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.connection.session;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessions;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.sessionsParallel;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.threadPool;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.databaseMgr;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SessionSteps {

    @When("connection open schema session for database: {word}")
    public void connection_open_schema_session_for_database(String name) {
        connection_open_schema_sessions_for_databases(list(name));
    }

    @When("connection open (data )session for database: {word}")
    public void connection_open_data_session_for_database(String name) {
        connection_open_data_sessions_for_databases(list(name));
    }

    @When("connection open schema session(s) for database(s):")
    public void connection_open_schema_sessions_for_databases(List<String> names) {
        for (String name : names) {
            sessions.add(databaseMgr.session(name, Arguments.Session.Type.SCHEMA));
        }
    }

    @When("connection open (data )session(s) for database(s):")
    public void connection_open_data_sessions_for_databases(List<String> names) {
        for (String name : names) {
            sessions.add(databaseMgr.session(name, Arguments.Session.Type.DATA));
        }
    }

    @When("connection open (data )sessions in parallel for databases:")
    public void connection_open_data_sessions_in_parallel_for_databases(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        for (String name : names) {
            sessionsParallel.add(CompletableFuture.supplyAsync(
                    () -> databaseMgr.session(name, Arguments.Session.Type.DATA), threadPool)
            );
        }
    }

    @When("connection close all sessions")
    public void connection_close_all_sessions() {
        for (TypeDB.Session session : sessions) {
            session.close();
        }
        sessions.clear();
    }

    @Then("session(s) is/are null: {bool}")
    public void sessions_are_null(Boolean isNull) {
        for (TypeDB.Session session : sessions) {
            assertEquals(isNull, isNull(session));
        }
    }

    @Then("session(s) is/are open: {bool}")
    public void sessions_are_open(Boolean isOpen) {
        for (TypeDB.Session session : sessions) {
            assertEquals(isOpen, session.isOpen());
        }
    }

    @Then("sessions in parallel are null: {bool}")
    public void sessions_in_parallel_are_null(Boolean isNull) {
        Stream<CompletableFuture<Void>> assertions = sessionsParallel
                .stream().map(futureSession -> futureSession.thenApplyAsync(session -> {
                    assertEquals(isNull, isNull(session));
                    return null;
                }));

        CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
    }

    @Then("sessions in parallel are open: {bool}")
    public void sessions_in_parallel_are_open(Boolean isOpen) {
        Stream<CompletableFuture<Void>> assertions = sessionsParallel.stream().map(
                futureSession -> futureSession.thenApplyAsync(session -> {
                    assertEquals(isOpen, session.isOpen());
                    return null;
                }));

        CompletableFuture.allOf(assertions.toArray(CompletableFuture[]::new)).join();
    }

    @Then("session(s) has/have database: {word}")
    public void sessions_have_database(String name) {
        sessions_have_databases(list(name));
    }

    @Then("session(s) has/have database(s):")
    public void sessions_have_databases(List<String> names) {
        assertEquals(names.size(), sessions.size());
        Iterator<TypeDB.Session> sessionIter = sessions.iterator();

        for (String name : names) {
            assertEquals(name, sessionIter.next().database().name());
        }
    }

    @Then("sessions in parallel have databases:")
    public void sessions_in_parallel_have_databases(List<String> names) {
        assertEquals(names.size(), sessionsParallel.size());
        Iterator<CompletableFuture<TypeDB.Session>> futureSessionIter = sessionsParallel.iterator();
        CompletableFuture<?>[] assertions = new CompletableFuture<?>[names.size()];

        int i = 0;
        for (String name : names) {
            assertions[i++] = futureSessionIter.next().thenApplyAsync(session -> {
                assertEquals(name, session.database().name());
                return null;
            });
        }

        CompletableFuture.allOf(assertions).join();
    }
}
