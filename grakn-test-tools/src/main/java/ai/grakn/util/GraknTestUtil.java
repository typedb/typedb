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

package ai.grakn.util;

import ai.grakn.GraknSystemProperty;

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
     * Gets a port which a service can bind to.
     */
    public static int getEphemeralPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
