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


import ai.grakn.GraknSystemProperty;
import ai.grakn.util.EmbeddedCassandra;
import ai.grakn.util.EmbeddedRedis;

/**
 * <p>
 *     Helps Setup Grakn Test Environment
 * </p>
 *
 * <p>
 *     Contains utility methods and statically initialized environment variables to control
 *     Grakn unit tests.
 * </p>
 *
 * @author borislav
 *
 */
public class GraknTestSetup {
    private static String CONFIG = GraknSystemProperty.TEST_PROFILE.value();

    /**
     * Starts cassandra if needed.
     */
    public static void startCassandraIfNeeded() {
        if (GraknTestSetup.usingJanus()) {
            EmbeddedCassandra.start();
        }
    }

    public static void startRedisIfNeeded(int port) {
        EmbeddedRedis.start(port);
    }

    /**
     *
     * @return true if the tests are running on tinker graph
     */
    public static boolean usingTinker() {
        return "tinker".equals(CONFIG);
    }

    /**
     *
     * @return true if the tests are running on janus graph.
     */
    public static boolean usingJanus() {
        return "janus".equals(CONFIG);
    }
}
