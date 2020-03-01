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

import grakn.core.server.Version;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static grakn.core.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsNotRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertZipExists;
import static grakn.core.distribution.DistributionE2EConstants.getLogsPath;
import static grakn.core.distribution.DistributionE2EConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Contains tests which must be ran against a running Grakn instance.
 *
 */
public class GraknGraqlCommands_WithARunningGraknE2E {
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
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    /**
     * test 'graql console' and 'define person sub entity;' from inside the console
     */
    @Test
    public void graql_shouldBeAbleToExecuteQuery_fromConsoleSession() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;\n";

        String output = commandExecutor
                .command("./grakn", "console", "-k", randomKeyspace)
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    @Test
    public void graql_shouldBeAbleToLoadFile_whenPathIsRelative() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;\n";
        String fileName = "test.gql";

        Files.write(Paths.get(fileName), "define person sub entity; insert $x isa person; match $x isa person; get;\n".getBytes());

        String output = getCommandExecutor(Paths.get(".").toAbsolutePath())
                .command(GRAKN_UNZIPPED_DIRECTORY.resolve("grakn").toAbsolutePath().toString(), "console", "-k", randomKeyspace, "-f", fileName)
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    /**
     * test 'grakn server clean' 'y' while grakn is running
     */
    @Test
    public void grakn_GraknServerCleanShouldNotBeAllowed_IfGraknIsRunning() throws IOException, InterruptedException, TimeoutException {
        String userInput = "y";
        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();

        assertThat(output, containsString("Grakn is still running! Please do a shutdown with 'grakn server stop' before performing a cleanup."));
    }

    /**
     * test 'grakn server status' when started
     */
    @Test
    public void grakn_testPrintStatus_whenCurrentlyRunning() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: RUNNING"), containsString("Grakn Core Server: RUNNING")));
    }

    /**
     * make sure Grakn is properly writing logs inside grakn.log file
     */
    @Test
    public void logMessagesArePrintedInLogFile() throws IOException {
        Path logsPath = getLogsPath();
        Path graknLogFilePath = logsPath.resolve("grakn.log");
        String logsString = new String(Files.readAllBytes(graknLogFilePath), StandardCharsets.UTF_8);
        assertThat(logsString, containsString("Grakn started"));
    }

    @Test
    public void graknServerVersion_shouldPrintCurrentVersion() throws InterruptedException, TimeoutException, IOException {
        String output = commandExecutor.command("./grakn", "server", "version").execute().outputUTF8();
        assertThat(output, containsString(Version.VERSION));
    }
}