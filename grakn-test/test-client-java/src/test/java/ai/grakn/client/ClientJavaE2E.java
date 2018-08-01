package ai.grakn.client;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static ai.grakn.client.ClientJavaE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests performing queries with the client-java
 *
 * @author Ganeshwara Herawan Hananda
 */
public class ClientJavaE2E {
    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_startGraknThenDefineSchema() throws IOException, InterruptedException, TimeoutException {
        unzipThenStartGrakn();
        defineSchema();
    }

    private static void unzipThenStartGrakn() throws IOException, InterruptedException, TimeoutException {
        ClientJavaE2EConstants.assertGraknStopped();
        ClientJavaE2EConstants.assertZipExists();
        ClientJavaE2EConstants.unzipGrakn();
        commandExecutor.command("./grakn", "server", "start").execute();
        ClientJavaE2EConstants.assertGraknRunning();
    }

    /**
     * TODO: verify
     */
    private static void defineSchema() throws IOException, InterruptedException, TimeoutException {
        String schema = "define marriage sub relationship, relates spouse;" +
                "define parentship sub relationship, relates parent, relates child;" +
                "define person sub entity, has name, plays spouse, plays child;";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(schema.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        ClientJavaE2EConstants.assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    /**
     * TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformInsert() throws IOException, InterruptedException, TimeoutException {
        String insert = "insert isa person has name \"Bill Gates\";" +
                "insert isa person has name \"Melinda Gates\";" +
                "insert isa person has name \"Jennifer Katharine Gates\";" +
                "insert isa person has name \"Phoebe Adele Gates\";" +
                "insert isa person has name \"Rory John Gates\";";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(insert.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }

    /**
     * TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformMatchGet() throws IOException, InterruptedException, TimeoutException {
        String matchGet = "match $p isa person, has name $n; get;";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }

    /**
     TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformMatchInsert() throws IOException, InterruptedException, TimeoutException {
        String matchInsert = "match $p isa person, has name \"Melinda Gates\"; insert $p has name \"Melinda Ann French\";";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(matchInsert.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }

    /**
     * TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformMatchDelete() throws IOException, InterruptedException, TimeoutException {
        String matchDelete = "match $n isa name \"Melinda Ann French\"; delete $n;";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(matchDelete.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }


    /**
     * TODO: match sibling via rule
     */


    /**
     * TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformMatchAggregate() throws IOException, InterruptedException, TimeoutException {
        String matchGet = "match $p isa person; aggregate count;";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }

    /**
     * TODO: verify
     */
    public void shouldBeAbleToPerformComputeCount() throws IOException, InterruptedException, TimeoutException {
        String matchGet = "compute count in person;";
        commandExecutor
                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }
}
