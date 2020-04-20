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

package grakn.core.test.behaviour.connection.keyspace;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static grakn.common.util.Collections.list;
import static grakn.core.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static grakn.core.test.behaviour.connection.ConnectionSteps.threadPool;
import static grakn.core.test.behaviour.server.ServerSetup.server;
import static org.junit.Assert.assertTrue;

public class KeyspaceSteps {

    @When("connection create keyspace: {word}")
    public void connection_create_keyspace(String name) {
        connection_create_keyspaces(list(name));
    }

    @When("connection create keyspace(s):")
    public void connection_create_keyspaces(List<String> names) {
        // TODO: This step should be rewritten once we can create keypsaces without opening sessions
        for (String name : names) {
            server.session(name);
        }
    }

    @When("connection create keyspaces in parallel:")
    public void connection_create_keyspaces_in_parallel(List<String> names) {
        assertTrue(THREAD_POOL_SIZE >= names.size());

        // TODO: This step should be rewritten once we can create keypsaces without opening sessions
        CompletableFuture[] creations = new CompletableFuture[names.size()];
        int i = 0;
        for (String name : names) {
            creations[i++] = CompletableFuture.supplyAsync(() -> server.session(name), threadPool);
        }

        CompletableFuture.allOf(creations).join();
    }

    // TODO re-enable these when we refactor `KeyspaceHandler` to not read gRPC messages directly, exposing it in the TestServer

    @When("connection delete keyspace(s):")
    public void connection_delete_keyspaces(List<String> names) {
//        for (String keyspaceName : names) {
//            server.keyspaces().delete(keyspaceName);
//        }
    }

    @When("connection delete keyspaces in parallel:")
    public void connection_delete_keyspaces_in_parallel(List<String> names) {
//        assertTrue(THREAD_POOL_SIZE >= names.size());
//
//        // TODO: This step should be rewritten once we can create keyspaces without opening sessions
//        CompletableFuture[] deletions = new CompletableFuture[names.size()];
//        int i = 0;
//        for (String name : names) {
//            deletions[i++] = CompletableFuture.supplyAsync(
//                    () -> { server.keyspaces().delete(name); return null; },
//                    threadPool
//            );
//        }
//
//        CompletableFuture.allOf(deletions).join();
    }

    @Then("connection has keyspace(s):")
    public void connection_has_keyspaces(List<String> names) {
//        assertEquals(set(names), set(server.keyspaces().retrieve()));
    }

    @Then("connection does not have keyspace(s):")
    public void connection_does_not_have_keyspaces(List<String> names) {
//        Set<String> keyspaces = set(server.keyspaces().retrieve());
//        for (String keyspaceName : names) {
//            assertFalse(keyspaces.contains(keyspaceName));
//        }
    }
}
