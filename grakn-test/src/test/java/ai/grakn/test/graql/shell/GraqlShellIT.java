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
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.GraqlShell;
import ai.grakn.test.EngineContext;
import ai.grakn.util.Schema;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mjson.Json;
import org.apache.commons.io.output.TeeOutputStream;
import org.hamcrest.Matcher;
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
import java.util.stream.Stream;

import static ai.grakn.test.GraknTestEnv.usingTinker;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
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

/*
These tests are failing for some reason. The websocket appears to close itself mid-test. Possibly the graphs are not being
cleared correctly, or engine is not starting/stopping correctly.
 */
@Ignore // TODO: Fix this test
public class GraqlShellIT {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    private static InputStream trueIn;
    private static PrintStream trueOut;
    private static PrintStream trueErr;
    private static final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = "/graql-test-history";

    private static final ImmutableList<String> keyspaces =
            ImmutableList.of(GraqlShell.DEFAULT_KEYSPACE, "foo", "bar", "batch");

    @BeforeClass
    public static void setUpClass() throws Exception {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;

        // Disable engine logs so we can test stdout
        ((Logger) org.slf4j.LoggerFactory.getLogger("ai.grakn.engine")).setLevel(Level.OFF);
    }

    @After
    public void tearDown() throws GraknValidationException {
        for (String keyspace : keyspaces){
            Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE).clear();
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

        assertShellMatches(ImmutableList.of("-k", "grakn"),
                "match im-in-the-default-keyspace sub entity; ask;",
                containsString("True")
        );
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
        testShell("", err, "-f", "src/test/graql/shell test(weird name).gql");
        assertEquals("", err.toString());
    }

    @Test
    public void testLoadCommand() throws Exception {
        assertShellMatches(
                "load src/test/graql/shell test(weird name).gql",
                anything(),
                "match movie sub entity; ask;",
                containsString("True")
        );
    }

    @Test
    public void testLoadCommandWithEscapes() throws Exception {
        assertShellMatches(
                "load src/test/graql/shell\\ test\\(weird\\ name\\).gql",
                anything(),
                "match movie sub entity; ask;",
                containsString("True")
        );
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
        assertShellMatches(
                "match $x isa relation; ask;",
                containsString("False")
        );
    }

    @Test
    public void testInsertQuery() throws Exception {
        assertShellMatches(
                "insert entity2 sub entity;",
                anything(),
                "match $x isa entity2; ask;",
                containsString("False"),
                "insert $x isa entity2;",
                anything(),
                "match $x isa entity2; ask;",
                containsString("True")
        );
    }

    @Test
    public void testInsertOutput() throws Exception {
        assertShellMatches(
                "insert X sub entity; $thingy isa X;",
                allOf(containsString("$thingy"), containsString("isa"), containsString("X"))
        );
    }

    @Test
    public void testAggregateQuery() throws Exception {
        assertShellMatches(
                "match $x sub concept; aggregate count;",
                is("8") // Expect to see the whole meta-ontology
        );
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
        assertThat(result, containsString(Schema.MetaSchema.RELATION.getLabel().getValue()));
    }

    @Test
    public void testReasonerOff() throws Exception {
        assertShellMatches(
                "insert man sub entity has name; name sub resource datatype string;",
                anything(),
                "insert person sub entity;",
                anything(),
                "insert has name 'felix' isa man;",
                anything(),
                "insert $my-rule isa inference-rule lhs {$x isa man;} rhs {$x isa person;};",
                anything(),
                "commit",
                "match isa person, has name $x;"
                // No results
        );
    }

    @Test
    public void testReasoner() throws Exception {
        assertShellMatches(ImmutableList.of("--infer"),
                "insert man sub entity has name; name sub resource datatype string;",
                anything(),
                "insert person sub entity;",
                anything(),
                "insert has name 'felix' isa man;",
                anything(),
                "match isa person, has name $x;",
                // No results
                "insert $my-rule isa inference-rule lhs {$x isa man;} rhs {$x isa person;};",
                anything(),
                "commit",
                "match isa person, has name $x;",
                containsString("felix") // Results after result is added
        );
    }

    @Test
    public void testInvalidQuery() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell("insert movie sub entity; $moon isa movie; $europa isa $moon;\n", err);

        assertThat(err.toString(), allOf(containsString("not"), containsString("type")));
    }

    @Test
    public void testComputeCount() throws Exception {
        assertShellMatches(
                "insert X sub entity; $a isa X; $b isa X; $c isa X;",
                anything(),
                "commit",
                "compute count;",
                is("3")
        );
    }

    @Test
    public void testRollback() throws Exception {
        // Tinker graph doesn't support rollback
        assumeFalse(usingTinker());

        String[] result = testShell("insert E sub entity;\nrollback\nmatch $x label E;\n").split("\n");

        // Make sure there are no results for match query
        assertEquals(">>> match $x label E;", result[result.length-2]);
        assertEquals(">>> ", result[result.length-1]);
    }

    @Test
    public void testLimit() throws Exception {
        assertShellMatches(
                "match $x sub concept; limit 1;",
                anything() // Only one result
        );
    }

    @Test
    public void testGraqlOutput() throws Exception {
        String result = testShell("", "-e", "match $x sub concept;", "-o", "graql");
        assertThat(result, allOf(containsString("$x"), containsString(Schema.MetaSchema.ENTITY.getLabel().getValue())));
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

        assertShellMatches(
                "insert X sub resource datatype string; value '" + value + "' isa X;",
                anything(),
                "match $x isa X;",
                allOf(containsString("$x"), containsString(value))
        );
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
        assertShellMatches(
                "insert X sub entity; R sub resource datatype string; X has R; isa X has R 'foo';",
                anything(),
                "match $x isa X;",
                allOf(containsString("id"), not(containsString("\"foo\"")))
        );
    }

    @Test
    public void testDisplayResourcesCommand() throws Exception {
        assertShellMatches(
                "insert X sub entity; R sub resource datatype string; X has R; isa X has R 'foo';",
                anything(),
                "display R;",
                "match $x isa X;",
                allOf(containsString("id"), containsString("\"foo\""))
        );
    }

    @Test
    public void whenRunningCleanCommand_TheGraphIsCleanedAndCommitted() throws Exception {
        assertShellMatches(
                "insert my-type sub entity;",
                is("{}"),
                "commit",
                "match $x sub entity;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "confirm",
                is("Cleaning..."),
                "match $x sub entity;",
                containsString("entity"),
                "rollback",
                "match $x sub entity;",
                containsString("entity")
        );
    }

    @Test
    public void whenCancellingCleanCommand_TheGraphIsNotCleaned() throws Exception {
        assertShellMatches(
                "insert my-type sub entity;",
                is("{}"),
                "match $x sub entity;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "n",
                is("Cancelling clean."),
                "match $x sub entity;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "no thanks bad idea thanks for warning me",
                is("Cancelling clean."),
                "match $x sub entity;",
                containsString("entity"),
                containsString("entity")
        );
    }

    @Test
    public void testExecuteMultipleQueries() throws Exception {
        assertShellMatches(
                "insert X sub entity; $x isa X; match $y isa X; match $y isa X; aggregate count;",
                // Make sure we see results from all three queries
                containsString("$x"),
                containsString("$y"),
                is("1")
        );
    }

    @Test
    public void whenRunningBatchLoad_LoadCompletes() throws Exception {
        testShell("", "-k", "batch", "-f", "src/test/graql/shell test(weird name).gql");
        testShell("", "-k", "batch", "-b", "src/test/graql/batch-test.gql");

        assertShellMatches(ImmutableList.of("-k", "batch"),
                "match $x isa movie; ask;",
                containsString("True")
        );
    }

    @Test
    public void whenRunningBatchLoadAndAnErrorOccurs_PrintStatus() throws Exception {
        testShell("", "-k", "batch", "-f", "src/test/graql/shell test(weird name).gql");

        assertShellMatches(ImmutableList.of("-k", "batch", "-b", "src/test/graql/batch-test-bad.gql"),
                is("Status of batch [1]: FAILED"),
                is("Number batches completed: 1"),
                containsString("Approximate queries executed:"),
                containsString("All tasks completed")
        );
    }

    @Test
    public void whenUserMakesAMistake_SubsequentQueriesStillWork() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        String out = testShell(
                "match $x sub concet; aggregate count;\n" +
                "match $x sub concept; ask;\n",
                err);

        assertThat(err.toString(), not(containsString("error")));
        assertThat(out, containsString("True"));
    }

    @Test
    public void testDuplicateRelation() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        testShell(
                "insert R sub relation, relates R1, relates R2; R1 sub role; R2 sub role;\n" +
                        "insert X sub entity, plays R1, plays R2;\n" +
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

    private void assertShellMatches(Object... matchers) throws Exception {
        assertShellMatches(ImmutableList.of(), matchers);
    }

    // Arguments should be strings or matchers. Strings are interpreted as input, matchers as expected output
    private void assertShellMatches(List<String> arguments, Object... matchers) throws Exception {
        String input = Stream.of(matchers)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .collect(joining("\n", "", "\n"));

        List<Matcher<? super String>> matcherList = Stream.of(matchers)
                .map(obj -> (obj instanceof Matcher) ? (Matcher<String>) obj : is(">>> " + obj))
                .collect(toList());

        String output = testShell(input, arguments.toArray(new String[arguments.size()]));

        List<String> outputLines = Lists.newArrayList(output.replace(" \r", "").split("\n"));

        ImmutableSet<String> noPromptArgs = ImmutableSet.of("-e", "-f", "-b", "-v", "-h");
        if (Sets.intersection(Sets.newHashSet(arguments), noPromptArgs).isEmpty()) {
            // Remove first four lines containing license and last line containing prompt
            outputLines = outputLines.subList(4, outputLines.size() - 1);
        }

        assertThat(outputLines, contains(matcherList));
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

        PrintStream out = new PrintStream(bout);

        // Intercept stderr, but make sure it is still printed using the TeeOutputStream
        PrintStream err = new PrintStream(new TeeOutputStream(berr, trueErr));

        try {
            System.out.flush();
            System.err.flush();
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

