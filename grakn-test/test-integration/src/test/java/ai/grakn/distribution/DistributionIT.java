package ai.grakn.distribution;

import ai.grakn.util.GraknVersion;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static ai.grakn.distribution.DistributionITConstants.GRAKN_UNZIPPED_DIRECTORY;
import static ai.grakn.distribution.DistributionITConstants.assertGraknRunning;
import static ai.grakn.distribution.DistributionITConstants.assertGraknStopped;
import static ai.grakn.distribution.DistributionITConstants.assertZipExists;
import static ai.grakn.distribution.DistributionITConstants.unzipGrakn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Grakn end to end test suite which verifies bootup functionalities, including:
 * - 'grakn server start, stop, and clean'
 * - 'graql console'
 * If you are testing functionalities which needs a running Grakn, add it in {@link DistributionWithARunningGraknIT} instead.
 * @author Ganeshwara Herawan Hananda
 */

public class DistributionIT {
    private ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @Before
    public void beforeEachTests_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertGraknStopped();
        assertZipExists();
        unzipGrakn();
    }

    @After
    public void afterEachTests_cleanupDistribution() throws IOException {
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void verifyDistributionFiles() {
        // assert files exist
        final Path grakn = GRAKN_UNZIPPED_DIRECTORY.resolve("grakn");
        final Path graql = GRAKN_UNZIPPED_DIRECTORY.resolve("graql");
        final Path graknProperties = GRAKN_UNZIPPED_DIRECTORY.resolve("conf").resolve("grakn.properties");
        final Path assetsDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("assets");
        final Path cassandraDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("cassandra");
        final Path graknDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("grakn");
        final Path libDirectory = GRAKN_UNZIPPED_DIRECTORY.resolve("services").resolve("lib");

        assertThat(grakn.toString() + " isn't present", grakn.toFile().exists(), equalTo(true));
        assertThat(graql.toString() + " isn't present", graql.toFile().exists(), equalTo(true));
        assertThat(graknProperties + " isn't present", graknProperties.toFile().exists(), equalTo(true));
        assertThat(assetsDirectory.toString() + " isn't present", assetsDirectory.toFile().exists(), equalTo(true));
        assertThat(cassandraDirectory.toString() + " isn't present", cassandraDirectory.toFile().exists(), equalTo(true));
        assertThat(graknDirectory.toString() + " isn't present", graknDirectory.toFile().exists(), equalTo(true));
        assertThat(libDirectory.toString() + " isn't present", libDirectory.toFile().exists(), equalTo(true));

        // assert if grakn and graql is chmod'ed properly
        assertThat(grakn.toString() + " isn't an executable", grakn.toFile().canExecute(), equalTo(true));
        assertThat(graql.toString() + " isn't an executable", graql.toFile().canExecute(), equalTo(true));
    }

    /**
     * test 'grakn server start' and 'grakn server stop'
     */
    @Test
    public void grakn_shouldBeAbleToStartAndStop() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
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
        assertGraknStopped();
        String output = commandExecutor.command("./grakn", "server", "status").execute().outputUTF8();
        assertThat(output, allOf(containsString("Storage: NOT RUNNING"), containsString("Engine: NOT RUNNING")));
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
        assertThat(output, allOf(containsString("Cleaning Storage...SUCCESS"), containsString("Cleaning Grakn...SUCCESS")));
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
     * test 'grakn version'
     */
    @Test
    public void grakn_shouldPrintVersion() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "version").execute().outputUTF8();
        assertThat(output, containsString(GraknVersion.VERSION));
    }

    /**
     * test 'grakn help'
     */
    @Test
    public void grakn_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "help").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn COMMAND"));
    }

    /**
     * test 'grakn <some-invalid-command>'
     */
    @Test
    public void grakn_whenReceivingInvalidCommand_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./grakn", "invalid-command").execute().outputUTF8();
        assertThat(output, containsString("Usage: grakn COMMAND"));
    }

    /**
     * test 'graql help'
     */
    @Test
    public void graql_shouldPrintHelp() throws IOException, InterruptedException, TimeoutException {
        String output = commandExecutor.command("./graql", "help").execute().outputUTF8();
        assertThat(output, containsString("Usage: graql COMMAND"));
    }
}
