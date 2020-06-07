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

package grakn.core.test.assembly;

import grakn.client.GraknClient;
import grakn.core.server.Version;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static grakn.core.test.assembly.AssemblyConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.test.assembly.AssemblyConstants.assertGraknIsNotRunning;
import static grakn.core.test.assembly.AssemblyConstants.assertGraknIsRunning;
import static grakn.core.test.assembly.AssemblyConstants.assertZipExists;
import static grakn.core.test.assembly.AssemblyConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for bootup and shutdown, cleaning, help menus, and other user-facing functionality
 * These functionalities are handled by the Grakn Daemon
 */
public class DaemonTest {
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
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        assertGraknIsNotRunning();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    /*
    Clean up state that may have been initialised in a test
     */
    @Before
    public void inCleanState() {
        assertGraknIsNotRunning();
    }

    /*
    Clean up state that may have been initialised in a test
     */
    @After
    public void stopGrakn() throws InterruptedException, TimeoutException, IOException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknIsNotRunning();
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
     * test 'grakn server clean' 'y' while grakn is running
     */
    @Test
    public void grakn_GraknServerCleanShouldNotBeAllowed_IfGraknIsRunning() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

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
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: RUNNING"), containsString("Grakn Core Server: RUNNING")));
    }

    @Test
    public void graknServerVersion_shouldPrintCurrentVersion() throws InterruptedException, TimeoutException, IOException {
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknIsRunning();

        String output = commandExecutor.command("./grakn", "server", "version").execute().outputUTF8();
        assertThat(output, containsString(Version.VERSION));
    }

    /**
     * test 'grakn server help'
     */
    @Test
    public void grakn_testPrintServerHelp_whenServerOff() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "server", "help").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn server COMMAND"));
    }

    /**
     * test 'grakn server status' when stopped
     */
    @Test
    public void grakn_testPrintStatus_whenServerOff() throws IOException, InterruptedException, TimeoutException {
        assertGraknIsNotRunning();
        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: NOT RUNNING"), containsString("Grakn Core Server: NOT RUNNING")));
    }

    /**
     * test 'grakn server clean' 'y'
     */
    @Test
    public void grakn_shouldBeAbleToExecuteGraknServerClean_Yes_whenServerOff() throws IOException, InterruptedException, TimeoutException {
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
    public void grakn_shouldBeAbleToExecuteGraknServerClean_No_whenServerOff() throws IOException, InterruptedException, TimeoutException {
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
    public void grakn_shouldBeAbleToExecuteGraknServerClean_Invalid_whenServerOff() throws IOException, InterruptedException, TimeoutException {
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
    public void grakn_shouldPrintHelp_whenServerOff() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "help").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }

    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidCommand_shouldPrintHelp_whenServerOff() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "invalid-command").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }


    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidServerCommand_shouldPrintHelp_whenServerOff() throws IOException, InterruptedException, TimeoutException {
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
