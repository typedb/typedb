package io.mindmaps.graql.api.shell;

import io.mindmaps.factory.MindmapsGraphFactory;
import io.mindmaps.factory.MindmapsTinkerGraphFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraqlShellTest {

    private MindmapsGraphFactory graphFactory;

    private String[] providedConf;
    private String expectedVersion = "graql-9.9.9";

    @Before
    public void setUp() {
        graphFactory = strings -> {
            providedConf = strings;
            return MindmapsTinkerGraphFactory.getInstance().newGraph();
        };
    }

    @Test
    public void testStartAndExitShell() throws IOException {
        // Assert simply that the shell starts and terminates without errors
        assertEquals(">>> exit\n", testShell("exit\n"));
    }

    @Test
    public void testHelpOption() throws IOException {
        String result = testShell("", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("graql.sh"), containsString("-e"),
                        containsString("--execute <arg>"), containsString("query to execute")
                )
        );
    }

    @Test
    public void testVersionOption() throws IOException {
        String result = testShell("", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    @Test
    public void testConfigOption() throws IOException {
        testShell("", "-c", "conf.properties");
        assertArrayEquals(new String[]{"conf.properties"}, providedConf);
    }

    @Test
    public void testExecuteOption() throws IOException {
        String result = testShell("", "-e", "match $x isa role-type ask");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    @Test
    public void testMatchQuery() throws IOException {
        String[] result = testShell("match $x isa type\nexit").split("\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x isa type", result[0]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws IOException {
        String result = testShell("match $x isa relation-type ask\n");
        assertThat(result, containsString("False"));
    }

    @Test
    public void testInsertQuery() throws IOException {
        String result = testShell(
                "match $x isa entity-type ask\ninsert my-type isa entity-type\nmatch $x isa entity-type ask\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    @Test
    public void testInsertOutput() throws IOException {
        String[] result = testShell("insert a-type isa entity-type; thingy isa a-type\n").split("\n");

        // Expect six lines output - one for the query, four results and a new prompt
        assertEquals(6, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[0]);
        assertEquals(">>> ", result[5]);

        assertThat(
                Arrays.toString(Arrays.copyOfRange(result, 1, 5)),
                allOf(containsString("a-type"), containsString("entity-type"), containsString("thingy"))
        );
    }

    @Test
    public void testAutocomplete() throws IOException {
        String result = testShell("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString("type"), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    @Test
    public void testAutocompleteShellCommand() throws IOException {
        String result = testShell("\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    @Test
    public void testInvalidQuery() throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie isa entity-type; moon isa movie; europa isa moon\n", err);

        assertThat(err.toString(), allOf(containsString("moon"), containsString("not"), containsString("type")));
    }

    private String testShell(String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream err, String... args) throws IOException {
        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream pout = new PrintStream(bout);
        PrintStream perr = new PrintStream(err);

        GraqlShell.runShell(args, graphFactory, expectedVersion, in, pout, perr);

        pout.flush();
        perr.flush();

        return bout.toString();
    }
}
