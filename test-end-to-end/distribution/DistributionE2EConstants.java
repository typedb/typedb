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

package grakn.core.distribution;

import grakn.core.common.config.Config;
import grakn.core.common.config.ConfigKey;
import org.junit.Assert;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DistributionE2EConstants {
    public static final Path GRAKN_BASE_DIR = Paths.get("/", "opt", "grakn", "core");
    public static final String GRAKN_BIN = Paths.get("/", "usr", "local", "bin", "grakn").toAbsolutePath().toString();

//    public static final Path GRAKN_BASE_DIR = Paths.get("/private/var/tmp/_bazel_lolski/84c573308050bc0236391991cc923148/execroot/graknlabs_grakn_core/bazel-out/darwin-fastbuild/genfiles/grakn-core-all-mac");
//    public static final String GRAKN_BIN = GRAKN_BASE_DIR.resolve("grakn").toAbsolutePath().toString(); // TODO: fix

    public static void assertGraknIsRunning() {
        Config config = Config.read(GRAKN_BASE_DIR.resolve("conf").resolve("grakn.properties"));
        boolean serverReady = isServerReady(config.getProperty(ConfigKey.SERVER_HOST_NAME), config.getProperty(ConfigKey.GRPC_PORT));
        assertThat("assertGraknRunning() failed because ", serverReady, equalTo(true));
    }

    public static void assertGraknIsNotRunning() {
        Config config = Config.read(GRAKN_BASE_DIR.resolve("conf").resolve("grakn.properties"));
        boolean serverReady = isServerReady(config.getProperty(ConfigKey.SERVER_HOST_NAME), config.getProperty(ConfigKey.GRPC_PORT));
        assertThat("assertGraknRunning() failed because ", serverReady, equalTo(false));
    }

    private static boolean isServerReady(String host, int port) {
        try {
            Socket s = new Socket(host, port);
            s.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
