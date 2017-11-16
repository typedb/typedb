package ai.grakn.dist;

import org.junit.After;
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
    private final ByteArrayInputStream inContent = new ByteArrayInputStream(new byte[]{});

    // TODO: test pid files
    // TODO: test log files

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Before
    public void setup() {
        underTest = new DistApplication(new Scanner(inContent),new PrintStream(outContent));

//        System.setOut(new PrintStream(outContent));
    }

    @After
    public void after() {
//        System.setOut(null);
    }

    @Ignore
    @Test
    public void main() {
        System.out.println("Checking if there are dangling processes ...");
        String queuePid = underTest.getPidOfQueue();
        String storagePid = underTest.getPidOfStorage();
        String graknPid = underTest.getPidOfGrakn();

        if(!graknPid.isEmpty()) {
            underTest.stopGrakn();
        }

        if(!queuePid.isEmpty()) {
            underTest.stopQueue();
        }

        if(!storagePid.isEmpty()) {
            underTest.stopStorage();
        }
    }

}
