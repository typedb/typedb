/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.test.behaviour.server;

import grakn.core.rule.GraknTestServer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Run Grakn Core and Cassandra using the GraknTestServer
 * The static 'server' field gives us a handle to the test server
 */
public class SingletonTestServer {
    private static GraknTestServer server = null;

    public static GraknTestServer get() {
        return server;
    }

    public static void shutdown() {
        server.after();
    }

    public static void start() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // use reflection to call the uninstallIfInstalled method on GoogleSecurityManager
        // this is not an issue if we use a @ClassRule from JUnit to run cassandra, because cassandra's manager installs first
        // however, running this setup method allows bazel test runner to install its own GoogleSecurityManager first
        // which is why we remove it before proceeding
        SecurityManager securityManager = System.getSecurityManager();
        Method uninstallGoogleManager = securityManager.getClass().getMethod("uninstallIfInstalled");
        uninstallGoogleManager.invoke(null);
        server = new GraknTestServer();
        server.before();
    }
}
