package ai.grakn.dist;

import ai.grakn.util.GraknVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class DistApplicationIT {

    private DistApplication underTest;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayInputStream inContent = null;

    // TODO: test pid files
    // TODO: test log files

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Before
    public void setup() {
        underTest = new DistApplication(new Scanner(inContent),new PrintStream(outContent));

        System.setOut(new PrintStream(outContent));
    }

    @After
    public void after() {
        System.setOut(null);
    }

    @Ignore
    @Test
    public void server_start() {
        DistApplication.main(new String[]{"server","start"});
        DistApplication.main(new String[]{"server","start","grakn"});
        DistApplication.main(new String[]{"server","start","queue"});
        DistApplication.main(new String[]{"server","start","storage"});
    }

    @Ignore
    @Test
    public void server_stop() {
        DistApplication.main(new String[]{"server","stop"});
        DistApplication.main(new String[]{"server","stop","grakn"});
        DistApplication.main(new String[]{"server","stop","queue"});
        DistApplication.main(new String[]{"server","stop","storage"});
    }
    @Ignore
    @Test
    public void server_status() {
        exit.checkAssertionAfterwards(() -> {
            Assert.assertTrue(outContent.toString().startsWith("Storage: "));
        });
        underTest.run(new String[]{"server","status"});
        underTest.run(new String[]{"server","status","--verbose"}); //TODO:
    }

    @Ignore
    @Test
    public void server_clean() {
        DistApplication.main(new String[]{"server","clean"});
    }
    @Ignore
    @Test
    public void server_help() {
        exit.checkAssertionAfterwards(() -> {
            Assert.assertTrue(outContent.toString().startsWith("Usage: grakn server COMMAND"));
        });
        underTest.run(new String[]{"server","help"});
    }

    @Ignore
    @Test
    public void noArguments() {
        exit.checkAssertionAfterwards(() -> {
            Assert.assertTrue(outContent.toString().startsWith("Usage: grakn COMMAND"));
        });
        underTest.run(new String[]{""});
    }

    @Ignore
    @Test
    public void wrongArguments() {
        exit.checkAssertionAfterwards(() -> {
            Assert.assertTrue(outContent.toString().startsWith("Usage: grakn COMMAND"));
        });
        underTest.run(new String[]{"WRONG ARG"});
    }

    @Ignore
    @Test
    public void version() {
        exit.expectSystemExitWithStatus(0);

        exit.checkAssertionAfterwards(() -> Assert.assertEquals(GraknVersion.VERSION+"\n", outContent.toString()));
        underTest.run(new String[]{"version"});
    }

    @Ignore
    @Test
    public void help() {
        exit.checkAssertionAfterwards(() -> {
            Assert.assertTrue(outContent.toString().startsWith("Usage: grakn COMMAND"));
        });
        underTest.run(new String[]{"help"});
    }

}
