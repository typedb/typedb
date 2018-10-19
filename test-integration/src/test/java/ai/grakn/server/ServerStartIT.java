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

package ai.grakn.test.server;

import ai.grakn.Keyspace;
import ai.grakn.test.rule.EmbeddedCassandraContext;
import ai.grakn.test.rule.ServerContext;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class ServerStartIT {

    public static EmbeddedCassandraContext cassandraContext = new EmbeddedCassandraContext();

    public static final ServerContext engine1 = new ServerContext();

    public static final ServerContext engine2 = new ServerContext();

    public static final ServerContext engine3 = new ServerContext();

    private static final Set<ServerContext> engines = new HashSet<>(Arrays.asList(engine1, engine2, engine3));

    @ClassRule
    public static RuleChain chain = RuleChain
            .outerRule(cassandraContext)
            .around(engine1)
            .around(engine2)
            .around(engine3);

    @Test
    public void whenStartingMultipleEngines_InitializationSucceeds() {
        HashSet<CompletableFuture<Void>> futures = new HashSet<>();

        //Check That They Running
        engines.forEach(engine -> futures.add(
                CompletableFuture.supplyAsync(engine::server).handle((result, exception) -> handleException(exception))
        ));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    }

    @Test
    public void whenStartingAndCreatingKeyspace_InitializationSucceeds() {
        engines.forEach(engine ->{
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