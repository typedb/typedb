package grakn.core.console.test;

import grakn.core.console.GraknConsole;
import grakn.core.console.exception.GraknConsoleException;
import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Test console without starting the Grakn test server
 */
public class StandaloneConsoleIT {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void consoleThrowsWhenNoServerRunning() throws ParseException, IOException, InterruptedException {
        expectedException.expect(GraknConsoleException.class);
        expectedException.expectMessage("Unable to create connection to Grakn instance at:");
        GraknConsole console = new GraknConsole(new String[0], null, null);
        console.run();
    }
}
