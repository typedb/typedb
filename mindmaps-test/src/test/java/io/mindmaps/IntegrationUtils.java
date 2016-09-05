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

package io.mindmaps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class IntegrationUtils {

    private static AtomicBoolean ENGINE_ON = new AtomicBoolean(false);

    private static void hideLogs() {
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    public static void startTestEngine() throws Exception {
        if (ENGINE_ON.compareAndSet(false, true)) {
            hideLogs();
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-embedded.yaml");
            MindmapsEngineServer.start();
            hideLogs();
            sleep(5000);
        }
    }
}
