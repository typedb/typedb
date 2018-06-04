package ai.grakn.distribution;

import ai.grakn.util.GraknVersion;
import org.junit.Assert;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DistributionE2EConstants {
    // path of grakn zip files
    public static final String ZIP_FILENAME = "grakn-dist-" + GraknVersion.VERSION + ".zip";
    public static final Path GRAKN_BASE_DIRECTORY = Paths.get(System.getProperty("main.basedir"));
    public static final Path GRAKN_TARGET_DIRECTORY = Paths.get(GRAKN_BASE_DIRECTORY.toString(), "grakn-dist", "target");
    public static final Path ZIP_FULLPATH = Paths.get(GRAKN_TARGET_DIRECTORY.toString(), ZIP_FILENAME);
    public static final Path GRAKN_UNZIPPED_DIRECTORY = Paths.get(GRAKN_TARGET_DIRECTORY.toString(), "grakn-dist-" + GraknVersion.VERSION);

    public static void assertGraknRunning() {
        assertThat("assertGraknRunning() failed because /tmp/grakn-engine.pid is not found", Paths.get("/tmp/grakn-engine.pid").toFile().exists(), equalTo(true));
        assertThat("assertGraknRunning() failed because /tmp/grakn-queue.pid is not found", Paths.get("/tmp/grakn-queue.pid").toFile().exists(), equalTo(true));
        assertThat("assertGraknRunning() failed because /tmp/grakn-storage.pid is not found", Paths.get("/tmp/grakn-storage.pid").toFile().exists(), equalTo(true));
    }

    public static void assertGraknStopped() {
        assertThat("assertGraknStopped() failed because /tmp/grakn-engine.pid exists", Paths.get("/tmp/grakn-engine.pid").toFile().exists(), equalTo(false));
        assertThat("assertGraknStopped() failed because /tmp/grakn-queue.pid exists", Paths.get("/tmp/grakn-queue.pid").toFile().exists(), equalTo(false));
        assertThat("assertGraknStopped() failed because /tmp/grakn-storage.pid exists", Paths.get("/tmp/grakn-storage.pid").toFile().exists(), equalTo(false));
    }

    public static void assertZipExists() {
        if(!ZIP_FULLPATH.toFile().exists()) {
            Assert.fail("Grakn distribution '" + ZIP_FULLPATH.toString() + "' could not be found. Please ensure it has been build (ie., run `mvn package`)");
        }
    }

    public static void unzipGrakn() throws IOException, InterruptedException, TimeoutException {
        new ProcessExecutor()
                .directory(GRAKN_TARGET_DIRECTORY.toFile())
                .command("unzip", ZIP_FULLPATH.toString()).execute();
    }
}
