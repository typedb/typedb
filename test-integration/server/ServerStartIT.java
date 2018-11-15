/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server;

import grakn.core.server.keyspace.Keyspace;
import grakn.core.rule.EmbeddedCassandraContext;
import grakn.core.rule.ServerContext;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class ServerStartIT {

    private static EmbeddedCassandraContext cassandraContext = new EmbeddedCassandraContext();

    private static final ServerContext server1 = new ServerContext();

    private static final ServerContext server2 = new ServerContext();

    private static final ServerContext server3 = new ServerContext();

    private static final Set<ServerContext> servers = new HashSet<>(Arrays.asList(server1, server2, server3));

    @ClassRule
    public static RuleChain chain = RuleChain
            .outerRule(cassandraContext)
            .around(server1)
            .around(server2)
            .around(server3);

    @Test
    public void whenStartingMultipleEngines_InitializationSucceeds() {
        HashSet<CompletableFuture<Void>> futures = new HashSet<>();

        //Check servers are running
        servers.forEach(engine -> futures.add(
                CompletableFuture.supplyAsync(engine::server).handle((result, exception) -> handleException(exception))
        ));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    }

    @Test
    public void whenStartingAndCreatingKeyspace_InitializationSucceeds() {
        servers.forEach(engine -> {
            engine.systemKeyspace().addKeyspace(Keyspace.of("grakn"));
        });
    }

    private Void handleException(Throwable exception) {
        if (exception != null) {
            exception.printStackTrace();
            fail("Could not initialize engine successfully");
        }
        return null;
    }
}