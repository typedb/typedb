package ai.grakn.distribution;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static ai.grakn.distribution.DistributionE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static ai.grakn.distribution.DistributionE2EConstants.assertGraknRunning;
import static ai.grakn.distribution.DistributionE2EConstants.assertGraknStopped;
import static ai.grakn.distribution.DistributionE2EConstants.assertZipExists;
import static ai.grakn.distribution.DistributionE2EConstants.unzipGrakn;

/**
 * Tests performing queries with the client-java and graql console
 *
 * @author Ganeshwara Herawan Hananda
 */
public class ExecuteQueryE2E {
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
     * TODO: test define queries (define person, has name, relationship parent-child, rule sibling)
     */

    /**
     * TODO: test insert person has name "Bill Gates", "Melinda Gates", "Jennifer Katharine Gates", "Phoebe Adele Gates", "Rory John Gates"
     */

    /**
     * TODO: match get
     */

    /**
     * TODO: match insert
     */

    /**
     * TODO: match delete
     */
    /**
     * TODO: match sibling via rule
     */
    /**
     * TODO: match aggregate count
     */

    /**
     * TODO: compute count
     */
}
