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
import io.mindmaps.MindmapsGraphFactory;
import io.mindmaps.engine.MindmapsEngineServer;
import org.junit.BeforeClass;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class AbstractEngineTest {
    private static final String CONFIG = System.getProperty("mindmaps.test-profile");
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

            MindmapsEngineServer.start();

            sleep(5000);
        }
    }

    public static MindmapsGraphFactory factoryWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            keyspace = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return Mindmaps.factory(Mindmaps.DEFAULT_URI, keyspace);
    }

    private static void startEmbeddedCassandra() {
        try {
            Class cl = Class.forName("org.cassandraunit.utils.EmbeddedCassandraServerHelper");

            hideLogs();

            //noinspection unchecked
            cl.getMethod("startEmbeddedCassandra", String.class).invoke(null, "cassandra-embedded.yaml");

            hideLogs();
            sleep(5000);
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
