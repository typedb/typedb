/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.MindmapsGraphFactory;
import io.mindmaps.engine.MindmapsEngineServer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public abstract class AbstractMindmapsEngineTest {
    protected static MindmapsGraphFactory factory;
    protected static MindmapsGraph graph;

    private static AtomicBoolean EMBEDDED_CASS_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    public static void startTestEngine() throws Exception {
        if (EMBEDDED_CASS_ON.compareAndSet(false, true)) {
            startEmbeddedCassandra();
        }

        MindmapsEngineServer.stop();
        sleep(5000);
        MindmapsEngineServer.start();
        sleep(5000);
    }

    public static MindmapsGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (MindmapsTest.usingOrientDB()) {
            keyspace = "memory";
        } else {
            keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace);
    }

    public static MindmapsGraph graphWithNewKeyspace() {
        MindmapsGraphFactory factory = factoryWithNewKeyspace();
        return factory.getGraph();
    }

    private static void startEmbeddedCassandra() {
        try {
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            hideLogs();
            sleep(5000);
        }
        catch (ClassNotFoundException ignored) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
