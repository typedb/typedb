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

package ai.grakn.test.graql.shell;

import ai.grakn.Grakn;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.GraqlShell;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mjson.Json;
import org.apache.commons.io.output.TeeOutputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class GraqlShellIT {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    private static InputStream trueIn;
    private static PrintStream trueOut;
    private static PrintStream trueErr;
    private static final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    private static final ImmutableList<String> keyspaces = ImmutableList.of(GraqlShell.DEFAULT_KEYSPACE, "foo", "bar");

    @BeforeClass
    public static void setUpClass() throws Exception {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;
    }

    @After
    public void tearDown() throws GraknValidationException {
        for (String keyspace : keyspaces){
            Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph().clear();
        }
    }

    @AfterClass
    public static void resetIO() {
        System.setIn(trueIn);
        System.setOut(trueOut);
        System.setErr(trueErr);
    }

    @Test
    public void testStartAndExitShell() throws Exception {
        // Assert simply that the shell starts and terminates without errors
        assertTrue(testShell("exit\n").matches("[\\s\\S]*>>> exit(\r\n?|\n)"));
    }

    @Test
    public void testHelpOption() throws Exception {
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
    public void testVersionOption() throws Exception {
        String result = testShell("", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    @Test
    public void testExecuteOption() throws Exception {
        String result = testShell("", "-e", "match $x isa entity; ask;");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    @Test
    public void testDefaultKeyspace() throws Exception {
        testShell("insert im-in-the-default-keyspace sub entity;\ncommit\n");

        String result = testShell("match im-in-the-default-keyspace sub entity; ask;\n", "-k", "grakn");
        assertThat(result, containsString("True"));
    }

    @Test
    public void testSpecificKeyspace() throws Exception {
        testShell("insert foo-foo sub entity;\ncommit\n", "-k", "foo");
        testShell("insert bar-bar sub entity;\ncommit\n", "-k", "bar");

        String fooFooinFoo = testShell("match foo-foo sub entity; ask;\n", "-k", "foo");
        String fooFooInBar = testShell("match foo-foo sub entity; ask;\n", "-k", "bar");
        String barBarInFoo = testShell("match bar-bar sub entity; ask;\n", "-k", "foo");
        String barBarInBar = testShell("match bar-bar sub entity; ask;\n", "-k", "bar");
        assertThat(fooFooinFoo, containsString("True"));
        assertThat(fooFooInBar, containsString("False"));
        assertThat(barBarInFoo, containsString("False"));
        assertThat(barBarInBar, containsString("True"));
    }

    @Test
    public void testFileOption() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("", err, "-f", "src/test/graql/shell-test.gql");
        assertEquals("", err.toString());
    }

    @Test
    public void testMatchQuery() throws Exception {
        String[] result = testShell("match $x sub concept;\nexit").split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x sub concept;", result[4]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws Exception {
        String result = testShell("match $x isa relation; ask;\n");
        assertThat(result, containsString("False"));
    }

    @Test
    public void testInsertQuery() throws Exception {
        String result = testShell(
                "insert entity2 sub entity; match $x isa entity2; ask;\ninsert $x isa entity2;\nmatch $x isa entity2; ask;\n"
        );
        assertThat(result, allOf(containsString("False"), containsString("True")));
    }

    @Test
    public void testInsertOutput() throws Exception {
        String result = testShell("insert X sub entity; $thingy isa X;\n");
        List<String> resultLines = Lists.newArrayList(result.split("\r\n?|\n"));

        // Expect seven lines output - four for the license, one for the query, one results and a new prompt
        //noinspection unchecked
        assertThat(resultLines, contains(
                anything(),
                anything(),
                anything(),
                anything(),
                is(">>> insert X sub entity; $thingy isa X;"),
                allOf(containsString("$thingy"), containsString("isa"), containsString("X")),
                is(">>> ")
        ));
    }

    @Test
    public void testAggregateQuery() throws Exception {
        String result = testShell("match $x sub concept; aggregate count;\n");

        // Expect to see the whole meta-ontology
        assertThat(result, containsString("\n8\n"));
    }

    @Test
    public void testAutocomplete() throws Exception {
        String result = testShell("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString("concept"), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    @Test
    public void testAutocompleteShellCommand() throws Exception {
        String result = testShell("\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    @Test
    public void testAutocompleteFill() throws Exception {
        String result = testShell("match $x sub concep\t;\n");
        assertThat(result, containsString(Schema.MetaSchema.RELATION.getName().getValue()));
    }

    @Test
    public void testReasonerOff() throws Exception {
        String result = testShell(
                "insert man sub entity has-resource name; person sub entity; name sub resource datatype string;\n" +
                        "insert has name 'felix' isa man;\n" +
                        "insert $my-rule isa inference-rule lhs {$x isa man;} rhs {$x isa person;};\n" +
                        "match isa person, has name $x;\n"
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match isa person, has name $x;")) {
                assertFalse(results[i + 1].contains("felix"));
            }
        }
    }

    @Test
    public void testReasoner() throws Exception {
        String result = testShell(
                "insert man sub entity has-resource name; person sub entity; name sub resource datatype string;\n" +
                "insert has name 'felix' isa man;\n" +
                "match isa person, has name $x;\n" +
                "insert $my-rule isa inference-rule lhs {$x isa man;} rhs {$x isa person;};\n commit\n" +
                "match isa person, has name $x;\n", "--infer"
        );

        // Make sure first 'match' query has no results and second has exactly one result
        String[] results = result.split("\n");
        int matchCount = 0;
        for (int i = 0; i < results.length; i ++) {
            if (results[i].contains(">>> match isa person, has name $x;")) {

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

        assertEquals(result, 2, matchCount);
    }

    @Test
    public void testInvalidQuery() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie sub entity; $moon isa movie; $europa isa $moon;\n", err);

        assertThat(err.toString(), allOf(containsString("not"), containsString("type")));
    }

    @Test
    public void testComputeCount() throws Exception {
        String result = testShell("insert X sub entity; $a isa X; $b isa X; $c isa X;\ncommit\ncompute count;\n");
        assertThat(result, containsString("\n3\n"));
    }

    @Test
    public void testRollback() throws Exception {
        // Tinker graph doesn't support rollback
        assumeFalse(usingTinker());

        String[] result = testShell("insert E sub entity;\nrollback\nmatch $x type-name E;\n").split("\n");

        // Make sure there are no results for match query
        assertEquals(">>> match $x type-name E;", result[result.length-2]);
        assertEquals(">>> ", result[result.length-1]);
    }

    @Test
    public void testLimit() throws Exception {
        String result = testShell("match $x sub concept; limit 1;\n");

        // Expect seven lines output - four for the license, one for the query, only one result and a new prompt
        assertEquals(result, 7, result.split("\n").length);
    }

    @Test
    public void testGraqlOutput() throws Exception {
        String result = testShell("", "-e", "match $x sub concept;", "-o", "graql");
        assertThat(result, allOf(containsString("$x"), containsString(Schema.MetaSchema.ENTITY.getName().getValue())));
    }

    @Test
    public void testJsonOutput() throws Exception {
        String[] result = testShell("", "-e", "match $x sub concept;", "-o", "json").split("\n");
        assertTrue("expected more than 5 results: " + Arrays.toString(result), result.length > 5);
        Json json = Json.read(result[0]);
        Json x = json.at("x");
        assertTrue(x.has("id"));
        assertFalse(x.has("isa"));
    }

    @Test
    public void testHALOutput() throws Exception {
        String[] result = testShell("", "-e", "match $x sub concept;", "-o", "hal").split("\n");
        assertTrue("expected more than 5 results: " + Arrays.toString(result), result.length > 5);
        Json json = Json.read(result[0]);
        Json x = json.at("x");
        assertTrue(x.has("_id"));
        assertTrue(x.has("_baseType"));
    }

    @Test
    public void testRollbackSemicolon() throws Exception {
        // Tinker graph doesn't support rollback
        assumeFalse(usingTinker());

        String result = testShell("insert entity2 sub entity; insert $x isa entity2;\nrollback;\nmatch $x isa entity;\n");
        String[] lines = result.split("\n");

        // Make sure there are no results for match query
        assertEquals(result, ">>> match $x isa entity;", lines[lines.length-2]);
        assertEquals(result, ">>> ", lines[lines.length-1]);
    }

    @Test
    public void testErrorWhenEngineNotRunning() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("", err, "-r", "localhost:7654");

        assertFalse(err.toString().isEmpty());
    }

    @Test
    @Ignore
    /* TODO: Fix this test
     * Sometimes we see this: "Websocket closed, code: 1005, reason: null".
     * Other times, JLine crashes when receiving certain input.
     */
    public void fuzzTest() throws Exception {
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            String input = randomString(i);
            try {
                testShellAllowErrors(input);
            } catch (Throwable e) {
                // We catch all exceptions so we can report exactly what input caused the error
                throw new RuntimeException("Error when providing the following input to shell: [" + input + "]", e);
            }
        }
    }

    @Test
    public void testLargeQuery() throws Exception {
        String value = Strings.repeat("really-", 100000) + "long-value";
        String[] result = testShell("insert X sub resource datatype string; value '" + value + "' isa X;\nmatch $x isa X;\n").split("\n");
        assertThat(result[result.length-2], allOf(containsString("$x"), containsString(value)));
    }

    @Test
    public void testCommitError() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String out = testShell("insert bob sub relation;\ncommit;\nmatch $x sub relation;\n", err);
        assertFalse(out, err.toString().isEmpty());
    }

    @Test
    public void testCommitErrorExecuteOption() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String out = testShell("", err, "-e", "insert bob sub relation;");
        assertFalse(out, err.toString().isEmpty());
    }

    @Test
    public void testDefaultDontDisplayResources() throws Exception {
        String result = testShell(
                "insert X sub entity; R sub resource datatype string; X has-resource R; isa X has R 'foo';\n" +
                "match $x isa X;\n"
        );

        // Confirm there is a result, but no resource value
        assertThat(result, allOf(containsString("id"), not(containsString("\"foo\""))));
    }

    @Test
    public void testDisplayResourcesCommand() throws Exception {
        String result = testShell(
                "insert X sub entity; R sub resource datatype string; X has-resource R; isa X has R 'foo';\n" +
                "display R;\n" +
                "match $x isa X;\n"
        );

        // Confirm there is a result, plus a resource value
        assertThat(result, allOf(containsString("id"), containsString("\"foo\"")));
    }

    @Test
    public void testExecuteMultipleQueries() throws Exception {
        String result = testShell("insert X sub entity; $x isa X; match $y isa X; match $y isa X; aggregate count;\n");

        String[] lines = result.split("\n");

        // Make sure we see results from all three queries
        assertThat(lines[lines.length-2], is("1"));
        assertThat(lines[lines.length-3], containsString("$y"));
        assertThat(lines[lines.length-4], containsString("$x"));
        assertThat(lines[lines.length-5], containsString(">>> insert X sub entity"));
    }

    @Test
    public void testDuplicateRelation() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(
                "insert R sub relation, has-role R1, has-role R2; R1 sub role; R2 sub role;\n" +
                        "insert X sub entity, plays-role R1, plays-role R2;\n" +
                        "insert $x isa X; (R1: $x, R2: $x) isa R;\n" +
                        "match $x isa X; insert (R1: $x, R2: $x) isa R;\n" +
                        "commit\n",
                err
        );

        assertThat(err.toString().toLowerCase(), allOf(
                anyOf(containsString("exists"), containsString("one or more")),
                containsString("relation")
        ));
    }

    private static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();

        random.ints().limit(length).forEach(i -> sb.append((char) i));

        return sb.toString();
    }

    private String testShell(String input, String... args) throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String result = testShell(input, err, args);
        String errMessage = err.toString();
        assertTrue("Error: \"" + errMessage + "\"", errMessage.isEmpty());
        return result;
    }

    private String testShellAllowErrors(String input, String... args) throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        return testShell(input, err, args);
    }

    private String testShell(String input, ByteArrayOutputStream berr, String... args) throws Exception {
        InputStream in = new ByteArrayInputStream(input.getBytes());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        // Intercept stderr and stdout, but make sure it is still printed using the TeeOutputStream
        PrintStream out = new PrintStream(new TeeOutputStream(bout, trueOut));
        PrintStream err = new PrintStream(new TeeOutputStream(berr, trueErr));

        try {
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);

            GraqlShell.runShell(args, expectedVersion, historyFile);
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

