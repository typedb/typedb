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

package ai.grakn.test.engine;

import ai.grakn.Keyspace;
import ai.grakn.test.rule.EngineContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class GraknEngineStartTest {
    private static final Set<EngineContext> engines = new HashSet<>();

    @ClassRule
    public static final EngineContext engine1 = EngineContext.create();

    @ClassRule
    public static final EngineContext engine2 = EngineContext.create();

    @ClassRule
    public static final EngineContext engine3 = EngineContext.create();

    @BeforeClass
    public static void groupEngines(){
        engines.add(engine1);
        engines.add(engine2);
        engines.add(engine3);
    }

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
