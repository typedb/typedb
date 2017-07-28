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
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Main class in charge of starting major components: cassandra, redis and engine
 *
 * @author Ganeshwara Herawan Hananda
 */

public class Grakn {
    public static void main(String[] args) throws IOException, InterruptedException {
        final Logger LOG = LoggerFactory.getLogger(Grakn.class);
        GraknEngineConfig prop = GraknEngineConfig.create();
        OperatingSystemCalls osCalls = new OperatingSystemCalls();

        // instantiate major components: cassandra, redis and engine
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(prop, osCalls, "");
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCalls, "");
        GraknEngineServer graknEngineServer = new GraknEngineServer(prop);

        // start major components: cassandra, redis and engine
        Triplet<Process, CompletableFuture<Void>, CompletableFuture<Void>> cassandra = cassandraSupervisor.start();
        Pair<Process, CompletableFuture<Void>> redis = redisSupervisor.start();

        CompletableFuture<Void> cassandraReady = cassandra.getValue1();
        CompletableFuture<Void> cassandraStopped = cassandra.getValue2();
        CompletableFuture<Void> redisStopped = redis.getValue1();

        // start engine only after cassandra and redis is started
        try {
            cassandraReady.get();
            graknEngineServer.start();
        } catch (ExecutionException e) {
            System.out.println("Unable to start Grakn!");
            LOG.error("Unable to start Grakn!", e);
        }

        // shutdown handler for cassandra, redis and engine. if one of them dies, so should the others
        // these processes are tightly coupled, therefore there's no point in keeping some alive when the others are dead
        Supplier<Void> shutdownGrakn = () -> {
            cassandra.getValue0().destroy();
            redis.getValue0().destroy();

            // kill engine with exit value dependent on whether cassandra and redis exited successfully
            int cassandraExitValue = cassandra.getValue0().exitValue();
            int redisExitValue = redis.getValue0().exitValue();
            int engineExitValue;
            if (cassandraExitValue != 0 || redisExitValue != 0) {
                engineExitValue = 1;
            } else {
                engineExitValue = 0;
            }
            Runtime.getRuntime().halt(engineExitValue);

            return null;
        };

        cassandraStopped.whenComplete((result, t) -> {
            LOG.info("Cassandra process is shutting down...");
            shutdownGrakn.get();
        });
        redisStopped.whenComplete((result, t) -> {
            LOG.info("Redis process is shutting down...");
            shutdownGrakn.get();
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Engine process is shutting down...");
            shutdownGrakn.get();
        }, "Grakn-shutdown")); // close  on SIGTERM
    }
}
