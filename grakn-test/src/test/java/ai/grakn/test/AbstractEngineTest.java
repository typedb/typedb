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

package ai.grakn.test;

import ai.grakn.Grakn;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.GraknEngineServer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.BeforeClass;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

/**
 * Abstract test class that automatically starts the backend and engine and provides a method to get a graph factory
 */
public abstract class AbstractEngineTest {
    private static final String CONFIG = System.getProperty("grakn.test-profile");
    private static AtomicBoolean ENGINE_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @BeforeClass
    public static void startTestEngine() throws Exception {
        if (ENGINE_ON.compareAndSet(false, true)) {
            if (usingTitan()) {
                startEmbeddedCassandra();
            }

            GraknEngineServer.start();

            sleep(5000);
        }
    }

    protected static GraknGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Grakn.factory(Grakn.DEFAULT_URI, keyspace);
    }

    private static void startEmbeddedCassandra() {
        try {
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            hideLogs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static boolean usingTinker() {
        return "tinker".equals(CONFIG);
    }

    protected static boolean usingTitan() {
        return "titan".equals(CONFIG);
    }

    protected static boolean usingOrientDB() {
        return "orientdb".equals(CONFIG);
    }
}
