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

package grakn.core.assembly;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static grakn.core.assembly.AssemblyConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.assembly.AssemblyConstants.assertGraknIsNotRunning;
import static grakn.core.assembly.AssemblyConstants.assertGraknIsRunning;
import static grakn.core.assembly.AssemblyConstants.assertZipExists;
import static grakn.core.assembly.AssemblyConstants.getLogsPath;
import static grakn.core.assembly.AssemblyConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verify the built distribution contains the required files, logs to the right places, etc.
 * The place for general "health checks" of the built Grakn distribution
 */
public class DistributionTest {
    private static ProcessExecutor getCommandExecutor(Path directory) {
        return new ProcessExecutor()
                .directory(directory.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true);
    }

    private static ProcessExecutor commandExecutor = getCommandExecutor(GRAKN_UNZIPPED_DIRECTORY);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertZipExists();
        unzipGrakn();
        assertGraknIsNotRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException {
        assertGraknIsNotRunning();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void verifyDistributionFiles() {
        // assert files exist
        final Path grakn = GRAKN_UNZIPPED_DIRECTORY.resolve("grakn");
        final Path graknProperties = GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("conf").resolve("grakn.properties");
        final Path cassandraDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("services").resolve("cassandra");
        final Path libDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("services").resolve("lib");

        assertThat(grakn.toString() + " isn't present", grakn.toFile().exists(), equalTo(true));
        assertThat(graknProperties + " isn't present", graknProperties.toFile().exists(), equalTo(true));
        assertThat(cassandraDirectory.toString() + " isn't present", cassandraDirectory.toFile().exists(), equalTo(true));
        assertThat(libDirectory.toString() + " isn't present", libDirectory.toFile().exists(), equalTo(true));

        // assert if grakn and graql is chmod'ed properly
        assertThat(grakn.toString() + " isn't an executable", grakn.toFile().canExecute(), equalTo(true));
    }

    /**
     * make sure Grakn is properly writing logs inside grakn.log file
     */
    @Test
    public void logMessagesArePrintedInLogFile() throws IOException, TimeoutException, InterruptedException {
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

        Path logsPath = getLogsPath();
        Path graknLogFilePath = logsPath.resolve("grakn.log");
        String logsString = new String(Files.readAllBytes(graknLogFilePath), StandardCharsets.UTF_8);
        assertThat(logsString, containsString("Grakn started"));
    }
}
