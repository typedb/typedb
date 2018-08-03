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

package ai.grakn.util;

import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSystemProperty;
import ai.grakn.engine.GraknConfig;
import spark.Service;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * <p>
 *     Houses common testing methods which are needed between different modules
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class GraknTestUtil {
    private static String CONFIG = GraknSystemProperty.TEST_PROFILE.value();

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

    /**
     * Allocates an unused port for Spark.
     *
     * <p>
     * This should always be called <i>immediately</i> before Spark starts in order to minimise a potential race
     * condition: in between finding an unused port and starting Spark, something else may steal the same port.
     * </p>
     *
     * <p>
     *     The correct way to solve this race condition is to specify the Spark port as 0. Then, Spark will allocate
     *     the port itself. However, there is an issue where {@link Service#port()} will always report 0 even after
     *     Spark has started.
     * </p>
     */
    public static void allocateSparkPort(GraknConfig config) {
        config.setConfigProperty(GraknConfigKey.SERVER_PORT, getEphemeralPort());
    }

    /**
     * Gets a port which a service can bind to.
     *
     * <p>
     *     The port returned by this method will be unused at the time of calling. However, at any point afterwards
     *     it is possible that something else will take the port.
     * </p>
     */
    private static int getEphemeralPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
