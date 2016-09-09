/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.shell;import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.graql.GraqlShell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

public class GraqlShellTest {

    private InputStream trueIn;
    private PrintStream trueOut;
    private PrintStream trueErr;

    private GraqlClientMock client;
    private String expectedVersion = "graql-9.9.9";

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
    public void testDefaultNamespace() throws IOException {
        testShell("");
        assertEquals("mindmaps", client.getNamespace());
    }

    @Test
    public void testSpecifiedNamespace() throws IOException {
        testShell("", "-n", "myspace");
        assertEquals("myspace", client.getNamespace());
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
        String result = testShell("", "-e", "match $x isa role-type ask");

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
        String[] result = testShell("insert a-type isa entity-type; thingy isa a-type\n").split("\r\n?|\n");

        // Expect ten lines output - four for the license, one for the query, four results and a new prompt
        assertEquals(10, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[4]);
        assertEquals(">>> ", result[9]);

        assertThat(
                Arrays.toString(Arrays.copyOfRange(result, 5, 9)),
                allOf(containsString("a-type"), containsString("entity-type"), containsString("thingy"))
        );
    }

    @Test
    public void testAggregateQuery() throws IOException {
        String result = testShell("match $x isa concept-type aggregate count\n");

        // Expect to see the whole meta-ontology
        assertThat(result, containsString("\n8\n"));
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
    public void testAutocompleteFill() throws IOException {
        String result = testShell("match $x isa concept-typ\t\n");
        assertThat(result, containsString("\"relation-type\""));
    }

    @Test
    public void testReasoner() throws IOException {
        String result = testShell(
                "insert man isa entity-type; person isa entity-type;\n" +
                        "insert 'felix' isa man;\n" +
                        "match $x isa person;\n" +
                        "insert my-rule isa inference-rule lhs {match $x isa man;} rhs {match $x isa person;};\n" +
                        "match $x isa person;\n"
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        int matchCount = 0;
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match $x isa person")) {

                if (matchCount == 0) {
                    // First 'match' result is before rule is added, so should have no results
                    assertFalse(results[i + 1].contains("felix"));
                } else {
                    // Second 'match' result is after rule is added, so should have a result
                    assertTrue(results[i + 1].contains("felix"));
                }

                matchCount ++;
            }
        }

        assertEquals(2, matchCount);
    }

    @Test
    public void testInvalidQuery() throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie isa entity-type; moon isa movie; europa isa moon\n", err);

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
                "insert x isa X; (R1 x, R2 x) isa R\n" +
                "insert x isa X; (R1 x, R2 x) isa R\n",
                err
        );

        assertThat(err.toString().toLowerCase(), allOf(containsString("exists"), containsString("relation")));
    }

    @Test
    public void testLimit() throws IOException {
        String result = testShell("match $x isa concept-type limit 1\n");

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

            GraqlShell.runShell(args, expectedVersion, client);
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();

        return bout.toString();
    }
}
