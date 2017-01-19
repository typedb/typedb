/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.shell;

import ai.grakn.graql.GraqlShell;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

// TODO: Fix randomly failing tests
// GraqlShellTest sometimes stalls with no output. This is probably related to the mock websocket
@Ignore
public class GraqlShellTest {

    private InputStream trueIn;
    private PrintStream trueOut;
    private PrintStream trueErr;

    private GraqlClientMock client;
    private final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    @Before
    public void setUp() {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;

        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);
    }

    @After
    public void resetIO() {
        System.setIn(trueIn);
        System.setOut(trueOut);
        System.setErr(trueErr);
    }

    @Test
    public void testStartAndExitShell() throws IOException {
        // Assert simply that the shell starts and terminates without errors
        assertTrue(testShell("exit\n").matches("[\\s\\S]*>>> exit(\r\n?|\n)"));
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
    public void testDefaultKeyspace() throws IOException {
        testShell("");
        assertEquals("grakn", client.getKeyspace());
    }

    @Test
    public void testSpecifiedKeyspace() throws IOException {
        testShell("", "-k", "myspace");
        assertEquals("myspace", client.getKeyspace());
    }

   @Test
    public void testDefaultUri() throws IOException {
        testShell("");
        assertEquals("ws://localhost:4567/shell/remote", client.getURI().toString());
    }

    @Test
    public void testSpecifiedUri() throws IOException {
        testShell("", "-u", "1.2.3.4:5678");
        assertEquals("ws://1.2.3.4:5678/shell/remote", client.getURI().toString());
    }

    @Test
    public void testExecuteOption() throws IOException {
        String result = testShell("", "-e", "match $x isa role-type; ask;");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    @Test
    public void testMatchQuery() throws IOException {
        String[] result = testShell("match $x isa type\nexit").split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x isa type", result[4]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws IOException {
        String result = testShell("match $x isa relation-type; ask;\n");
        assertThat(result, containsString("False"));
    }

    @Test
    public void testInsertQuery() throws IOException {
        String result = testShell(
                "match $x isa entity-type; ask;\ninsert my-type isa entity-type;\nmatch $x isa entity-type; ask;\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    @Test
    public void testInsertOutput() throws IOException {
        String[] result = testShell("insert a-type isa entity-type; thingy isa a-type\n").split("\r\n?|\n");

        // Expect six lines output - four for the license, one for the query, no results and a new prompt
        assertEquals(6, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[4]);
        assertEquals(">>> ", result[5]);
    }

    @Test
    public void testAggregateQuery() throws IOException {
        String result = testShell("match $x isa type; aggregate count;\n");

        // Expect to see the whole meta-ontology
        assertThat(result, containsString("\n2\n"));
    }

    @Test
    public void testAutocomplete() throws IOException {
        String result = testShell("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString(Schema.MetaSchema.RELATION.getName().getValue()), containsString("match"),
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
    public void testAutocompleteFill() throws IOException {
        // The typo is deliberate because this is an auto-complete test
        String result = testShell("match $x sub typ\t;\n");
        assertThat(result, containsString("\"relation-type\""));
    }

    @Test
    public void testReasoner() throws IOException {
        ByteArrayOutputStream berr = new ByteArrayOutputStream();
        String result = testShell(
                "insert man isa entity-type; person isa entity-type;\n" +
                        "insert isa man;\n" +
                        "match $x isa person;\n" +
                        "insert isa inference-rule lhs {$x isa man;} rhs {$x isa person;};\n" +
                        "match $x isa person;\n", berr
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        int matchCount = 0;
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match $x isa person;")) {

                if (matchCount == 0) {
                    // First 'match' result is before rule is added, so should have no results
                    assertFalse(results[i + 1].startsWith("$x"));
                } else {
                    // Second 'match' result is after rule is added, so should have a result
                    assertTrue(results[i + 1].startsWith("$x"));
                }

                matchCount ++;
            }
        }

        assertEquals(2, matchCount);
    }

    @Test
    public void testInvalidQuery() throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie isa entity-type; moon isa movie; europa isa moon;\n", err);

        assertThat(err.toString(), allOf(containsString("moon"), containsString("not"), containsString("type")));

        // Errors should not be printed "in quotes"
        assertThat(err.toString(), not(containsString("\"\n")));
    }

    @Test
    public void testDuplicateRelation() throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(
                "insert R isa relation-type, has-role R1, has-role R2; R1 isa role-type; R2 isa role-type;\n" +
                "insert X isa entity-type, plays-role R1, plays-role R2;\n" +
                "insert x isa X; (R1: x, R2: x) isa R;\n" +
                "insert x isa X; (R1: x, R2: x) isa R;\n",
                err
        );

        assertThat(err.toString().toLowerCase(), allOf(containsString("exists"), containsString("relation")));
    }

    @Test
    public void testLimit() throws IOException {
        String result = testShell("match $x isa type; limit 1;\n");

        // Expect seven lines output - four for the license, one for the query, only one result and a new prompt
        assertEquals(result, 7, result.split("\n").length);
    }

    @Test
    public void fuzzTest() throws IOException {
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            testShell(randomString(i));
        }
    }

    @Test
    public void testLargeQuery() throws IOException {
        String id = Strings.repeat("really-", 1000) + "long-id";
        String[] result = testShell("insert X isa entity-type; '" + id + "' isa X;\nmatch $x isa X;\n").split("\n");
        assertThat(result[result.length-2], allOf(containsString("$x"), containsString(id)));
    }

    private String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        random.ints().limit(length).forEach(i -> sb.append((char) i));

        return sb.toString();
    }

    private String testShell(String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream berr, String... args) throws IOException {
        client = new GraqlClientMock();

        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        PrintStream err = new PrintStream(berr);

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);

            GraqlShell.runShell(args, expectedVersion, historyFile, client);
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();

        return bout.toString();
    }
}
