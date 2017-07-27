/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine;

import ai.grakn.engine.externalcomponents.CassandraSupervisor;
import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.engine.externalcomponents.RedisSupervisor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Main class in charge of starting major components: cassandra, redis and engine
 *
 * @author Ganeshwara Herawan Hananda
 */

public class Grakn {
    public static void main(String[] args) throws IOException, InterruptedException {
        GraknEngineConfig prop = GraknEngineConfig.create();
        OperatingSystemCalls osCalls = new OperatingSystemCalls();

        // instantiate major components: cassandra, redis and engine
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(prop, osCalls, "");
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCalls, "");
        GraknEngineServer graknEngineServer = new GraknEngineServer(prop);

        // start major components: cassandra, redis and engine
        Map.Entry<Process, CompletableFuture<Void>> cassandra = cassandraSupervisor.startSync();
        Map.Entry<Process, CompletableFuture<Void>> redis = redisSupervisor.startSync();

        cassandraSupervisor.waitForCassandraStarted();
        System.out.println("Cassandra started. Starting engine...");
        graknEngineServer.start();
        System.out.println("Cassandra started. Engine started");
        // shutdown major components: cassandra, redis and engine
        Supplier<Void> shutdownGrakn = () -> {
            cassandra.getKey().destroy();
            redis.getKey().destroy();
            Runtime.getRuntime().halt(0);
            return null;
        };

        cassandra.getValue().whenComplete((result, t) -> {
            System.out.println("Cassandra process is shutting down.");
            shutdownGrakn.get();
        });
        redis.getValue().whenComplete((result, t) -> {
            System.out.println("Redis process is shutting down.");
            shutdownGrakn.get();
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Engine process is shutting down.");
            shutdownGrakn.get();
        }, "Grakn-shutdown")); // close  on SIGTERM
    }
}
