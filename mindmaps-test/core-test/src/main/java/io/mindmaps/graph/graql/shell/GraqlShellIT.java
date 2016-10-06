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

package io.mindmaps.graph.graql.shell;

import com.google.common.base.Strings;
import io.mindmaps.graql.GraqlClientImpl;
import io.mindmaps.graql.GraqlShell;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GraqlShellIT {
    private static InputStream trueIn;
    private static PrintStream trueOut;
    private static PrintStream trueErr;
    private static String expectedVersion = "graql-9.9.9";

    private static void setUp() throws Exception {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;
    }

    private static void resetIO() {
        System.setIn(trueIn);
        System.setOut(trueOut);
        System.setErr(trueErr);
    }

    public static void testStartAndExitShell(String keyspace) throws IOException {
        // Assert simply that the shell starts and terminates without errors
        assertTrue(testShell(keyspace, "exit\n").matches("[\\s\\S]*>>> exit(\r\n?|\n)"));
    }

    public static void testHelpOption(String keyspace) throws IOException {
        String result = testShell(keyspace, "", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("graql.sh"), containsString("-e"),
                        containsString("--execute <arg>"), containsString("query to execute")
                )
        );
    }

    public static void testVersionOption(String keyspace) throws IOException {
        String result = testShell(keyspace, "", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    public static void testExecuteOption(String keyspace) throws IOException {
        String result = testShell(keyspace, "", "-e", "match $x isa role-type; ask;");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    public static void testFileOption(String keyspace) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(keyspace, "", err, "-f", "src/test/graql/shell-test.gql");
        assertEquals(err.toString(), "");
    }

    public static void testMatchQuery(String keyspace) throws IOException {
        String[] result = testShell(keyspace, "match $x isa type\nexit").split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x isa type", result[4]);
        assertTrue(result.length > 5);
    }

    public static void testAskQuery(String keyspace) throws IOException {
        String result = testShell(keyspace, "match $x isa relation-type; ask;\n");
        assertThat(result, containsString("False"));
    }

    public static void testInsertQuery(String keyspace) throws IOException {
        String result = testShell(keyspace,
                "match $x isa entity-type; ask;\ninsert my-type isa entity-type;\nmatch $x isa entity-type; ask;\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    public static void testInsertOutput(String keyspace) throws IOException {
        String[] result = testShell(keyspace, "insert a-type isa entity-type; thingy isa a-type\n").split("\r\n?|\n");

        // Expect six lines output - four for the license, one for the query, no results and a new prompt
        assertEquals(6, result.length);
        assertEquals(">>> insert a-type isa entity-type; thingy isa a-type", result[4]);
        assertEquals(">>> ", result[5]);
    }

    public static void testAutocomplete(String keyspace) throws IOException {
        String result = testShell(keyspace, "match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString("type"), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    public static void testAutocompleteShellCommand(String keyspace) throws IOException {
        String result = testShell(keyspace, "\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    public static void testAutocompleteFill(String keyspace) throws IOException {
        String result = testShell(keyspace, "match $x ako typ\t;\n");
        assertThat(result, containsString("\"relation-type\""));
    }

    public static void testReasoner(String keyspace) throws IOException {
        String result = testShell(keyspace,
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

    public static void testInvalidQuery(String keyspace) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(keyspace, "insert movie isa entity-type; moon isa movie; europa isa moon;\n", err);

        assertThat(err.toString(), allOf(containsString("moon"), containsString("not"), containsString("type")));
    }

    public static void testComputeCount(String keyspace) throws IOException {
        String result = testShell(keyspace, "insert X isa entity-type; a isa X; b isa X; c isa X;\ncommit\ncompute count;\n");
        assertThat(result, containsString("\n3\n"));
    }

    public static void testRollback(String keyspace) throws IOException {
        String[] result = testShell(keyspace, "insert E isa entity-type;\nrollback\nmatch $x isa entity-type\n").split("\n");

        // Make sure there are no results for match query
        assertEquals(">>> match $x isa entity-type", result[result.length-2]);
        assertEquals(">>> ", result[result.length-1]);
    }

    public static void testErrorWhenEngineNotRunning(String keyspace) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(keyspace, "", err, "-u", "localhost:7654");

        assertFalse(err.toString().isEmpty());
    }

    public static void fuzzTest(String keyspace) throws IOException {
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            System.out.println(i);
            testShell(keyspace, randomString(i));
        }
    }

    public static void testLargeQuery(String keyspace) throws IOException {
        String id = Strings.repeat("really-", 100000) + "long-id";
        String[] result = testShell(keyspace, "insert X isa entity-type; '" + id + "' isa X;\nmatch $x isa X;\n").split("\n");
        assertThat(result[result.length-2], allOf(containsString("$x"), containsString(id)));
    }

    private static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        random.ints().limit(length).forEach(i -> sb.append((char) i));

        return sb.toString();
    }

    private static String testShell(String keyspace, String input, String... args) throws IOException {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(keyspace, input, err, args);
    }

    private static String testShell(String keyspace, String input, ByteArrayOutputStream berr, String... args) throws IOException {
        try {
            setUp();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String[] newArgs = Arrays.copyOf(args, args.length + 2);
        newArgs[newArgs.length-2] = "-n";
        newArgs[newArgs.length-1] = keyspace;

        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bout);
        PrintStream err = new PrintStream(berr);

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);
            
            GraqlShell.runShell(newArgs, expectedVersion, new GraqlClientImpl());
        } catch (Exception e) {
            System.setErr(trueErr);
            e.printStackTrace();
            err.flush();
            fail(berr.toString());
        } finally {
            resetIO();
        }

        out.flush();
        err.flush();

        return bout.toString();
    }
}

