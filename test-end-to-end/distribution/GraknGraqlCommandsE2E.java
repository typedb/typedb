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

package ai.grakn.distribution;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static ai.grakn.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static ai.grakn.distribution.DistributionE2EConstants.assertGraknRunning;
import static ai.grakn.distribution.DistributionE2EConstants.assertGraknStopped;
import static ai.grakn.distribution.DistributionE2EConstants.assertZipExists;
import static ai.grakn.distribution.DistributionE2EConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

///**
// * Grakn end to end test suite which verifies bootup functionalities, including:
// * - 'grakn server start, stop, and clean'
// * - 'graql console'
// * If you are testing functionalities which needs a running Grakn, add it in {@link GraknGraqlCommands_WithARunningGraknE2E} instead.
// * @author Ganeshwara Herawan Hananda
// */

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
        assertGraknStopped();
    }

    @After
    public void afterEachTests_cleanupDistribution() throws IOException {
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void verifyDistributionFiles() {
        // assert files exist
        final Path grakn = GRAKN_UNZIPPED_DIRECTORY.resolve("grakn-core");
        final Path graknProperties = GRAKN_UNZIPPED_DIRECTORY.resolve("conf").resolve("grakn.properties");
        final Path assetsDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("assets");
        final Path cassandraDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("cassandra");
        final Path libDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("lib");

        assertThat(grakn.toString() + " isn't present", grakn.toFile().exists(), equalTo(true));
        assertThat(graknProperties + " isn't present", graknProperties.toFile().exists(), equalTo(true));
        assertThat(assetsDirectory.toString() + " isn't present", assetsDirectory.toFile().exists(), equalTo(true));
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
        commandExecutor.command("./grakn-core", "server", "start").execute();
        assertGraknRunning();
        commandExecutor.command("./grakn-core", "server", "stop").execute();
        assertGraknStopped();
    }

    /**
     * test 'grakn server help'
     */
    @Test
    public void grakn_testPrintServerHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn-core", "server", "help").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn-core server COMMAND"));
    }

    /**
     * test 'grakn server status' when stopped
     */
    @Test
    public void grakn_testPrintStatus_whenCurrentlyStopped() throws IOException, InterruptedException, TimeoutException {
        assertGraknStopped();
        String output = commandExecutor.command("./grakn-core", "server", "status").execute().outputUTF8();
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
                .command("./grakn-core", "server", "clean").execute().outputUTF8();
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
                .command("./grakn-core", "server", "clean").execute().outputUTF8();
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
                .command("./grakn-core", "server", "clean").execute().outputUTF8();
        assertThat(output, containsString("Canceling clean operation"));
    }

    /**
     * test 'grakn help'
     */
    @Test
    public void grakn_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn-core", "help").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }

    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidCommand_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn-core", "invalid-command").execute().outputUTF8();
        assertThat(output, containsString("Invalid argument:"));
    }
}
