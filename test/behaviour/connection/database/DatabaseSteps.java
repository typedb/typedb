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

package com.vaticle.typedb.core.test.behaviour.connection.database;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.databaseMgr;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.threadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatabaseSteps {

    @When("connection create database: {word}")
    public void connection_create_database(String name) {
        connection_create_databases(list(name));
    }

    @When("connection create database(s):")
    public void connection_create_databases(List<String> names) {
        for (String name : names) {
            databaseMgr.create(name);
        }
    }

    @When("connection create databases in parallel:")
    public void connection_create_databases_in_parallel(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        CompletableFuture<?>[] creations = new CompletableFuture<?>[names.size()];
        int i = 0;
        for (String name : names) {
            creations[i++] = CompletableFuture.supplyAsync(() -> databaseMgr.create(name), threadPool);
        }

        CompletableFuture.allOf(creations).join();
    }

    @When("connection delete database: {word}")
    public void connection_delete_database(String name) {
        connection_delete_databases(list(name));
    }

    @When("connection delete database(s):")
    public void connection_delete_databases(List<String> names) {
        for (String databaseName : names) {
            databaseMgr.get(databaseName).delete();
        }
    }

    @Then("connection delete database; throws exception: {word}")
    public void connection_delete_database_throws_exception(String name) {
        connection_delete_databases_throws_exception(list(name));
    }

    @Then("connection delete database(s); throws exception")
    public void connection_delete_databases_throws_exception(List<String> names) {
        for (String databaseName : names) {
            try {
                databaseMgr.get(databaseName).delete();
                fail();
            } catch (Exception e) {
                // successfully failed
            }
        }
    }

    @When("connection delete databases in parallel:")
    public void connection_delete_databases_in_parallel(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        CompletableFuture<?>[] deletions = new CompletableFuture<?>[names.size()];
        int i = 0;
        for (String name : names) {
            deletions[i++] = CompletableFuture.supplyAsync(
                    () -> {
                        databaseMgr.get(name).delete();
                        return null;
                    },
                    threadPool
            );
        }

        CompletableFuture.allOf(deletions).join();
    }

    @When("connection has database: {word}")
    public void connection_has_database(String name) {
        connection_has_databases(list(name));
    }

    @Then("connection has database(s):")
    public void connection_has_databases(List<String> names) {
        assertEquals(set(names),
                     databaseMgr.all().stream()
                             .map(database -> database.name())
                             .collect(Collectors.toSet()));
    }

    @Then("connection does not have database: {word}")
    public void connection_does_not_have_database(String name) {
        connection_does_not_have_databases(list(name));
    }

    @Then("connection does not have database(s):")
    public void connection_does_not_have_databases(List<String> names) {
        for (String name : names) {
            assertNull(databaseMgr.get(name));
        }
    }
}
