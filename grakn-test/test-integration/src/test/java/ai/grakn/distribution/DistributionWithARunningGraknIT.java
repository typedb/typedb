package ai.grakn.distribution;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static ai.grakn.distribution.DistributionITConstants.assertGraknRunning;
import static ai.grakn.distribution.DistributionITConstants.assertGraknStopped;
import static ai.grakn.distribution.DistributionITConstants.assertZipExists;
import static ai.grakn.distribution.DistributionITConstants.unzipGrakn;
import static ai.grakn.distribution.DistributionITConstants.GRAKN_UNZIPPED_DIRECTORY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A suite to put tests which needs a running Grakn.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class DistributionWithARunningGraknIT {

    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertGraknStopped();
        assertZipExists();
        unzipGrakn();
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    /**
     * test 'graql console' and 'define person sub entity;' from inside the console
     */
    @Test
    public void graql_shouldBeAbleToExecuteQuery_fromRepl() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;\n";

        String expected = "{}";

        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(graql.getBytes(StandardCharsets.UTF_8)))
                .command("./graql", "console", "-k", randomKeyspace).execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    /**
     * test "graql console -e 'define person sub entity;"
     */
    @Test
    public void graql_shouldBeAbleToExecuteQuery_fromArgument() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String graql = "define person sub entity; insert $x isa person; match $x isa person; get;";

        String output = commandExecutor.command("./graql", "console", "-k", randomKeyspace, "-e", graql).execute().outputUTF8();

        assertThat(output, allOf(containsString("$x"), containsString("id"), containsString("isa"), containsString("person")));
    }

    /**
     * test aggregate queries on marriage data
     */
    @Test
    public void graql_testAggregateQueries_onMarriageData() throws IOException, InterruptedException, TimeoutException {
        String randomKeyspace = "keyspace_" + UUID.randomUUID().toString().replace("-", "");
        String insert = "define person sub entity, has identifier; identifier sub attribute, datatype string;\n" +
                "define spouse1 sub role; spouse2 sub role; person plays spouse1; person plays spouse2;\n" +
                "define marriage sub relationship, relates spouse1, relates spouse2, has \"date\"; \"date\" sub attribute datatype string;\n" +
                "commit;\n" +
                "\n" +
                "insert isa person, has identifier \"Andrew Smith\";\n" +
                "insert isa person, has identifier \"Catherine Shaw\";\n" +
                "insert isa person, has identifier \"Paula Carter\";\n" +
                "insert isa person, has identifier \"Scott Jones\";\n" +
                "commit;\n" +
                "match $s1 has identifier \"Andrew Smith\"; $s2 has identifier \"Catherine Shaw\"; insert (spouse1: $s1, spouse2: $s2) isa marriage has \"date\" \"01-01-1980\";\n" +
                "commit;\n";
        String countPerson = "match $p isa person, has identifier $i; aggregate count;\n";
        String countMarriage = "match (spouse1: $x, spouse2: $y) isa marriage, has \"date\" $d; $x has identifier $xi; $y has identifier $yi; aggregate count;\n";

        String queries = insert + countPerson + countMarriage;

        String output = commandExecutor
                .redirectInput(new ByteArrayInputStream(queries.getBytes(StandardCharsets.UTF_8)))
                .command("./graql", "console", "-k", randomKeyspace).execute().outputUTF8();

        String[] split = output.split("\n");
        Long aggregateCountPerson = Long.parseLong(split[split.length-4]);
        Long aggregateCountMarriage = Long.parseLong(split[split.length-2]);

        assertThat(aggregateCountPerson, equalTo(4L));
        assertThat(aggregateCountMarriage, equalTo(1L));
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
        assertThat(output, allOf(containsString("Storage: RUNNING"), containsString("Engine: RUNNING")));
    }
}