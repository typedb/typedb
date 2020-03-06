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
    public static final Path GRAKN_TARGET_DIRECTORY = Paths.get(".").toAbsolutePath();
    public static final Path ZIP_FULLPATH = Paths.get(GRAKN_TARGET_DIRECTORY.toString(), "grakn-core-all-mac.zip");
    public static final Path GRAKN_UNZIPPED_DIRECTORY = Paths.get(GRAKN_TARGET_DIRECTORY.toString(), "distribution-test", "grakn-core-all-mac");

    public static void assertGraknIsRunning() {
        Config config = Config.read(GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("conf").resolve("grakn.properties"));
        boolean serverReady = isServerReady(config.getProperty(ConfigKey.SERVER_HOST_NAME), config.getProperty(ConfigKey.GRPC_PORT));
        assertThat("assertGraknRunning() failed because ", serverReady, equalTo(true));
    }

    public static void assertGraknIsNotRunning() {
        Config config = Config.read(GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("conf").resolve("grakn.properties"));
        boolean serverReady = isServerReady(config.getProperty(ConfigKey.SERVER_HOST_NAME), config.getProperty(ConfigKey.GRPC_PORT));
        assertThat("assertGraknIsNotRunning() failed because ", serverReady, equalTo(false));
    }

    public static void assertZipExists() {
        if(!ZIP_FULLPATH.toFile().exists()) {
            Assert.fail("Grakn distribution '" + ZIP_FULLPATH.toAbsolutePath().toString() + "' could not be found. Please ensure it has been build (ie., run `bazel build //:assemble-mac-zip`)");
        }
    }

    public static void unzipGrakn() throws IOException, InterruptedException, TimeoutException {
        new ProcessExecutor()
                .command("unzip", ZIP_FULLPATH.toString(), "-d", GRAKN_UNZIPPED_DIRECTORY.getParent().toString()).execute();
    }

    public static Path getLogsPath(){
        Config config = Config.read(GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("conf").resolve("grakn.properties"));
        Path logsPath = Paths.get(config.getProperty(ConfigKey.LOG_DIR));
        return logsPath.isAbsolute() ? logsPath : GRAKN_UNZIPPED_DIRECTORY.resolve(logsPath);
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
