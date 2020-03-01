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

import grakn.client.GraknClient;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static grakn.core.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsNotRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertGraknIsRunning;
import static grakn.core.distribution.DistributionE2EConstants.assertZipExists;
import static grakn.core.distribution.DistributionE2EConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

///**
// * Grakn end to end test suite which verifies bootup functionalities, including:
// * - 'grakn server start, stop, and clean'
// * - 'graql console'
// * If you are testing functionalities which needs a running Grakn, add it in GraknGraqlCommands_WithARunningGraknE2E instead.
//// */

public class GraknGraqlCommandsE2E {
    private ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @Before
    public void beforeEachTests_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertZipExists();
        unzipGrakn();
        assertGraknIsNotRunning();
    }

    @After
    public void afterEachTests_cleanupDistribution() throws IOException {
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
     * test 'grakn server start' and 'grakn server stop'
     */
    @Test
    public void grakn_shouldBeAbleToStartAndStop() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();
    }

    /**
     * test 'grakn server start --benchmark'
     */
    @Test
    public void grakn_shouldBeAbleToStartWithBenchmarkOption() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "start", "--benchmark").execute();
        assertGraknIsRunning();
        commandExecutor.command("./grakn", "server", "stop").execute();
    }

    /**
     * test 'grakn server help'
     */
    @Test
    public void grakn_testPrintServerHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "server", "help").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn server COMMAND"));
    }

    /**
     * test 'grakn server status' when stopped
     */
    @Test
    public void grakn_testPrintStatus_whenCurrentlyStopped() throws IOException, InterruptedException, TimeoutException {
        assertGraknIsNotRunning();
        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: NOT RUNNING"), containsString("Grakn Core Server: NOT RUNNING")));
    }

    /**
     * test 'grakn server clean' 'y'
     */
    @Test
    public void grakn_shouldBeAbleToExecuteGraknServerClean_Yes() throws IOException, InterruptedException, TimeoutException {
        String userInput = "y";
        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();
        assertThat(output, allOf(containsString("Cleaning Storage...SUCCESS"), containsString("Cleaning Grakn Core Server...SUCCESS")));
    }

    /**
     * test 'grakn server clean' 'N'
     */
    @Test
    public void grakn_shouldBeAbleToExecuteGraknServerClean_No() throws IOException, InterruptedException, TimeoutException {
        String userInput = "N";
        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();
        assertThat(output, containsString("Canceling clean operation"));
    }

    /**
     * test 'grakn server clean' 'n'
     */
    @Test
    public void grakn_shouldBeAbleToExecuteGraknServerClean_Invalid() throws IOException, InterruptedException, TimeoutException {
        String userInput = "n";
        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();
        assertThat(output, containsString("Canceling clean operation"));
    }

    /**
     * test 'grakn help'
     */
    @Test
    public void grakn_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "help").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }

    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidCommand_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "invalid-command").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }


    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidServerCommand_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "server", "start", "storag").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn server COMMAND\n"));
    }


    /**
     * Grakn should stop correctly when there are client connections still open
     */

    @Test
    public void grakn_whenThereAreOpenConnections_shouldBeAbleToStop() throws InterruptedException, TimeoutException, IOException {
        commandExecutor.command("./grakn", "server", "start").execute();
        String host = "localhost:48555";
        GraknClient graknClient = new GraknClient(host);
        GraknClient.Transaction test = graknClient.session("test").transaction().write();
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();
    }

    @Test
    public void grakn_shouldBeAbleToExecuteGraknServerClean_withCustomDbDirectory() throws IOException, TimeoutException, InterruptedException {
        // modify the path to the `db` folder in `grakn.properties`
        final Path graknProperties = GRAKN_UNZIPPED_DIRECTORY.resolve("server").resolve("conf").resolve("grakn.properties");
        final String keyspace = "custom_db_test";

        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(graknProperties.toFile())) {
            prop.load(input);
        }
        prop.setProperty("data-dir", "server/new-db/");
        graknProperties.toFile().setWritable(true);
        try (OutputStream output = new FileOutputStream(graknProperties.toFile())) {
            prop.store(output, null);
        }

        // start Grakn
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

        // insert some data
        String graql = "define person sub entity; insert $x isa person;\ncommit\n";
        commandExecutor
                .command("./grakn", "console", "-k", keyspace)
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .execute().outputUTF8();

        graql = "compute count;\n";
        String output = commandExecutor
                .command("./grakn", "console", "-k", keyspace)
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .execute().outputUTF8();

        // verify that the keyspace is not empty
        assertThat(output, containsString("compute count;\n1"));

        // stop Grakn
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();

        // clean Grakn
        String userInput = "y";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(userInput.getBytes(StandardCharsets.UTF_8)))
                .command("./grakn", "server", "clean").execute().outputUTF8();

        // start Grakn
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

        // verify that the keyspace is empty
        graql = "compute count;\n";
        output = commandExecutor
                .command("./grakn", "console", "-k", keyspace)
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .execute().outputUTF8();

        assertThat(output, containsString("compute count;\n0"));

        // stop Grakn
        commandExecutor.command("./grakn", "server", "stop").execute();
    }
}
