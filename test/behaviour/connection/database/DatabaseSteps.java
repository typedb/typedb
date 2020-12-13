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

package grakn.core.test.behaviour.connection.database;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static grakn.core.test.behaviour.connection.ConnectionSteps.grakn;
import static grakn.core.test.behaviour.connection.ConnectionSteps.threadPool;
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
            grakn.databases().create(name);
        }
    }

    @When("connection create databases in parallel:")
    public void connection_create_databases_in_parallel(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        final CompletableFuture<?>[] creations = new CompletableFuture<?>[names.size()];
        int i = 0;
        for (String name : names) {
            creations[i++] = CompletableFuture.supplyAsync(() -> grakn.databases().create(name), threadPool);
        }

        CompletableFuture.allOf(creations).join();
    }

    @When("connection delete database(s):")
    public void connection_delete_databases(List<String> names) {
        for (String databaseName : names) {
            grakn.databases().get(databaseName).delete();
        }
    }

    @Then("connection delete database(s); throws exception")
    public void connection_delete_databases_throws_exception(List<String> names) {
        for (String databaseName : names) {
            try {
                grakn.databases().get(databaseName).delete();
                fail();
            } catch (Exception e) {
                // successfully failed
            }
        }
    }

    @When("connection delete databases in parallel:")
    public void connection_delete_databases_in_parallel(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        final CompletableFuture<?>[] deletions = new CompletableFuture<?>[names.size()];
        int i = 0;
        for (String name : names) {
            deletions[i++] = CompletableFuture.supplyAsync(
                    () -> {
                        grakn.databases().get(name).delete();
                        return null;
                    },
                    threadPool
            );
        }

        CompletableFuture.allOf(deletions).join();
    }

    @Then("connection has database(s):")
    public void connection_has_databases(List<String> names) {
        assertEquals(set(names),
                     grakn.databases().all().stream()
                             .map(database -> database.name())
                             .collect(Collectors.toSet()));
    }

    @Then("connection does not have database(s):")
    public void connection_does_not_have_databases(List<String> names) {
        for (String name : names) {
            assertNull(grakn.databases().get(name));
        }
    }
}
