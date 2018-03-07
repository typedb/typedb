/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.graql.GraqlShell;
import ai.grakn.graql.internal.shell.ErrorMessage;
import ai.grakn.test.rule.DistributionContext;
import ai.grakn.util.GraknTestUtil;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mjson.Json;
import org.apache.commons.io.output.TeeOutputStream;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class GraqlShellIT {

    @ClassRule
    public static final DistributionContext dist = DistributionContext.create().inheritIO(true);
    private static InputStream trueIn;
    private static PrintStream trueOut;
    private static PrintStream trueErr;
    private static final String expectedVersion = "graql-9.9.9";
    private static final String historyFile = StandardSystemProperty.JAVA_IO_TMPDIR.value() + "/graql-test-history";

    private static int keyspaceSuffix = 0;

    private static boolean showStdOutAndErr = true;

    private final static int NUM_METATYPES = 4;

    @BeforeClass
    public static void setUpClass() throws Exception {
        trueIn = System.in;
        trueOut = System.out;
        trueErr = System.err;

        // TODO: Get these tests working consistently on Jenkins - causes timeouts
        assumeFalse(GraknTestUtil.usingJanus());
    }

    @Before
    public void changeSuffix() {
        keyspaceSuffix += 1;
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
        assertTrue(runShellWithoutErrors("exit\n").matches("[\\s\\S]*>>> exit(\r\n?|\n)"));
    }

    @Test
    public void testHelpOption() throws Exception {
        String result = runShellWithoutErrors("", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("graql console"), containsString("-e"),
                        containsString("--execute <arg>"), containsString("query to execute")
                )
        );
    }

    @Test
    public void testVersionOption() throws Exception {
        String result = runShellWithoutErrors("", "--version");
        assertThat(result, containsString(expectedVersion));
    }

    @Test
    public void testExecuteOption() throws Exception {
        String result = runShellWithoutErrors("", "-e", "match $x isa entity; aggregate ask;");

        // When using '-e', only results should be printed, no prompt or query
        assertThat(result, allOf(containsString("False"), not(containsString(">>>")), not(containsString("match"))));
    }

    @Test
    public void whenUsingExecuteOptionAndPassingGetQueriesWithoutVariables_PrintWarning() throws Exception {
        ShellResponse response = runShell("", "-e", "match sub entity; get;");

        // There should still be a result...
        assertThat(response.out(), containsString("{}"));

        // ...but also a warning
        assertThat(response.err(), containsString(ErrorMessage.NO_VARIABLE_IN_QUERY.getMessage()));
    }

    @Test
    public void whenUsingExecuteOptionAndPassingNonGetQueriesWithoutVariables_DoNotPrintWarning() throws Exception {
        // There should be no errors...
        String result = runShellWithoutErrors("", "-e", "define person sub entity;");

        // ...and a result
        assertThat(result, containsString("{}"));
    }

    @Test
    public void testDefaultKeyspace() throws Exception {
        runShellWithoutErrors("define im-in-the-default-keyspace sub entity;\ncommit\n");

        assertShellMatches(ImmutableList.of("-k", "grakn"),
                "match im-in-the-default-keyspace sub entity; aggregate ask;",
                containsString("True")
        );
    }

    @Test
    public void testSpecificKeyspace() throws Exception {
        runShellWithoutErrors("define foo-foo sub entity;\ncommit\n", "-k", "foo");
        runShellWithoutErrors("define bar-bar sub entity;\ncommit\n", "-k", "bar");

        String fooFooinFoo = runShellWithoutErrors("match foo-foo sub entity; aggregate ask;\n", "-k", "foo");
        String fooFooInBar = runShellWithoutErrors("match foo-foo sub entity; aggregate ask;\n", "-k", "bar");
        String barBarInFoo = runShellWithoutErrors("match bar-bar sub entity; aggregate ask;\n", "-k", "foo");
        String barBarInBar = runShellWithoutErrors("match bar-bar sub entity; aggregate ask;\n", "-k", "bar");
        assertThat(fooFooinFoo, containsString("True"));
        assertThat(fooFooInBar, containsString("False"));
        assertThat(barBarInFoo, containsString("False"));
        assertThat(barBarInBar, containsString("True"));
    }

    @Test
    public void testFileOption() throws Exception {
        ShellResponse response = runShell("", "-f", "src/test/graql/shell test(weird name).gql");
        assertEquals("", response.err());
    }

    @Test
    public void testLoadCommand() throws Exception {
        assertShellMatches(
                "load src/test/graql/shell test(weird name).gql",
                anything(),
                "match movie sub entity; aggregate ask;",
                containsString("True")
        );
    }

    @Test
    public void testLoadCommandWithEscapes() throws Exception {
        assertShellMatches(
                "load src/test/graql/shell\\ test\\(weird\\ name\\).gql",
                anything(),
                "match movie sub entity; aggregate ask;",
                containsString("True")
        );
    }

    @Test
    public void testMatch() throws Exception {
        String[] result = runShellWithoutErrors(
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;\nexit"
        ).split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertEquals(">>> match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;", result[4]);
        assertTrue(result.length > 5);
    }

    @Test
    public void testAskQuery() throws Exception {
        assertShellMatches(
                "match $x isa " + Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()+ "; aggregate ask;",
                containsString("False")
        );
    }

    @Test
    public void testInsertQuery() throws Exception {
        assertShellMatches(
                "define entity2 sub entity;",
                anything(),
                "match $x isa entity2; aggregate ask;",
                containsString("False"),
                "insert $x isa entity2;",
                anything(),
                "match $x isa entity2; aggregate ask;",
                containsString("True")
        );
    }

    @Test
    public void testInsertOutput() throws Exception {
        assertShellMatches(
                "define X sub entity; insert $thingy isa X;",
                containsString("{}"),
                allOf(containsString("$thingy"), containsString("isa"), containsString("X"))
        );
    }

    @Test
    public void testAggregateQuery() throws Exception {
        assertShellMatches(
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; aggregate count;",
                is(Integer.toString(NUM_METATYPES))
        );
    }

    @Test
    public void testAutocomplete() throws Exception {
        String result = runShellWithoutErrors("match $x isa \t");

        // Make sure all the autocompleters are working (except shell commands because we are writing a query)
        assertThat(
                result,
                allOf(
                        containsString(Schema.MetaSchema.THING.getLabel().getValue()), containsString("match"),
                        not(containsString("exit")), containsString("$x")
                )
        );
    }

    @Test
    public void testAutocompleteShellCommand() throws Exception {
        String result = runShellWithoutErrors("\t");

        // Make sure all the autocompleters are working (including shell commands because we are not writing a query)
        assertThat(result, allOf(containsString("type"), containsString("match"), containsString("exit")));
    }

    @Test
    public void testAutocompleteFill() throws Exception {
        String result = runShellWithoutErrors("match $x sub thin\t; get;\n");
        assertThat(result, containsString(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()));
    }

    @Test
    public void testReasonerOff() throws Exception {
        assertShellMatches(ImmutableList.of("--no_infer"),
                "define man sub entity has name; name sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " datatype string;",
                anything(),
                "define person sub entity;",
                anything(),
                "insert has name 'felix' isa man;",
                anything(),
                "define my-rule sub rule when {$x isa man;} then {$x isa person;};",
                anything(),
                "commit",
                "match isa person, has name $x; get;"
                // No results
        );
    }

    @Test
    public void testReasoner() throws Exception {
        assertShellMatches(
                "define man sub entity has name; name sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " datatype string;",
                anything(),
                "define person sub entity;",
                anything(),
                "insert has name 'felix' isa man;",
                anything(),
                "match isa person, has name $x; get;",
                // No results
                "define my-rule sub rule when {$x isa man;} then {$x isa person;};",
                anything(),
                "commit",
                "match isa person, has name $x; get;",
                containsString("felix") // Results after result is added
        );
    }

    @Test
    public void testInvalidQuery() throws Exception {
        ShellResponse response = runShell(
                "define movie sub entity; insert $moon isa movie; $europa isa $moon;\n"
        );

        assertThat(response.err(), allOf(containsString("not"), containsString("type")));
    }

    @Test
    public void testComputeCount() throws Exception {
        assertShellMatches(
                "define X sub entity; insert $a isa X; $b isa X; $c isa X;",
                anything(),
                anything(),
                "commit",
                "compute count;",
                is("3")
        );
    }

    @Test
    public void testRollback() throws Exception {
        // Tinker graph doesn't support rollback
        assumeFalse(GraknTestUtil.usingTinker());

        String[] result = runShellWithoutErrors("insert E sub entity;\nrollback\nmatch $x label E;\n").split("\n");

        // Make sure there are no results for get query
        assertEquals(">>> match $x label E; get;", result[result.length-2]);
        assertEquals(">>> ", result[result.length-1]);
    }

    @Test
    public void testLimit() throws Exception {
        assertShellMatches(
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; limit 1; get;",
                anything() // Only one result
        );
    }

    @Test
    public void testGraqlOutput() throws Exception {
        String result = runShellWithoutErrors(
                "", "-e", "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;", "-o", "graql"
        );
        assertThat(result, allOf(containsString("$x"), containsString(Schema.MetaSchema.ENTITY.getLabel().getValue())));
    }

    @Test
    public void testJsonOutput() throws Exception {
        String[] result = runShellWithoutErrors(
                "", "-e", "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;", "-o", "json"
        ).split("\n");
        assertThat(result, arrayWithSize(NUM_METATYPES));
        Json json = Json.read(result[0]);
        Json x = json.at("x");
        assertTrue(x.has("id"));
        assertFalse(x.has("isa"));
    }

    @Test
    public void testRollbackSemicolon() throws Exception {
        // Tinker graph doesn't support rollback
        assumeFalse(GraknTestUtil.usingTinker());

        String result = runShellWithoutErrors(
                "insert entity2 sub entity; insert $x isa entity2;\nrollback;\nmatch $x isa entity;\n"
        );
        String[] lines = result.split("\n");

        // Make sure there are no results for get query
        assertEquals(result, ">>> match $x isa entity; get;", lines[lines.length-2]);
        assertEquals(result, ">>> ", lines[lines.length-1]);
    }

    @Test
    public void whenEngineIsNotRunning_ShowAnError() throws Exception {
        ShellResponse response = runShell("", "-r", "localhost:7654");

        assertThat(response.err(), containsString(ErrorMessage.COULD_NOT_CONNECT.getMessage()));
    }

    @Test
    @Ignore
    /* TODO: Fix this test
     * Sometimes, JLine crashes when receiving certain input.
     */
    public void fuzzTest() throws Exception {
        int repeats = 100;
        for (int i = 0; i < repeats; i ++) {
            String input = randomString(i);
            try {
                runShell(input);
            } catch (Throwable e) {
                // We catch all exceptions so we can report exactly what input caused the error
                throw new RuntimeException("Error when providing the following input to shell: [" + input + "]", e);
            }
        }
    }

    @Test
    public void testLargeQuery() throws Exception {
        // We don't show output for this test because the query is really-really-really-really-really-really-really long
        showStdOutAndErr = false;

        try {
            String value = Strings.repeat("really-", 100000) + "long-value";

            assertShellMatches(
                    "define X sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " datatype string; insert val '" + value + "' isa X;",
                    anything(),
                    anything(),
                    "match $x isa X; get;",
                    allOf(containsString("$x"), containsString(value))
            );
        } finally {
            showStdOutAndErr = true;
        }
    }

    @Test
    public void whenErrorIsLarge_UserStillSeesEntireErrorMessage() throws Exception {
        // We don't show output for this test because the query is really-really-really-really-really-really-really long
        showStdOutAndErr = false;

        try {
            String value = Strings.repeat("really-", 100000) + "long-value";

            // Query has a syntax error
            ShellResponse response = runShell(
                    "insert X sub resource datatype string; value '" + value + "' isa X;\n"
            );

            assertThat(response.err(), allOf(containsString("syntax error"), containsString(value)));
        } finally {
            showStdOutAndErr = true;
        }
    }

    @Test
    public void testCommitError() throws Exception {
        ShellResponse response = runShell("insert bob sub relation;\ncommit;\nmatch $x sub relationship;\n");
        assertFalse(response.out(), response.err().isEmpty());
    }

    @Test
    public void testCommitErrorExecuteOption() throws Exception {
        ShellResponse response = runShell("", "-e", "insert bob sub relation;");
        assertFalse(response.out(), response.err().isEmpty());
    }

    @Test
    public void testDefaultDontDisplayResources() throws Exception {
        assertShellMatches(
                "define X sub entity; R sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " datatype string; X has R; insert isa X has R 'foo';",
                anything(),
                anything(),
                "match $x isa X; get;",
                allOf(containsString("id"), not(containsString("\"foo\"")))
        );
    }

    @Test
    public void testDisplayResourcesCommand() throws Exception {
        assertShellMatches(
                "define X sub entity; R sub " + Schema.MetaSchema.ATTRIBUTE.getLabel().getValue() + " datatype string; X has R; insert isa X has R 'foo';",
                anything(),
                anything(),
                "display R;",
                "match $x isa X; get;",
                allOf(containsString("id"), containsString("\"foo\""))
        );
    }

    @Test
    public void whenRunningCleanCommand_TheGraphIsCleanedAndCommitted() throws Exception {
        assertShellMatches(
                "define my-type sub entity;",
                is("{}"),
                "commit",
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "confirm",
                is("Cleaning..."),
                "match $x sub entity; get;",
                containsString("entity"),
                "rollback",
                "match $x sub entity; get;",
                containsString("entity")
        );
    }

    @Test
    public void whenCancellingCleanCommand_TheGraphIsNotCleaned() throws Exception {
        assertShellMatches(
                "define my-type sub entity;",
                is("{}"),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "n",
                is("Cancelling clean."),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                is("Are you sure? This will clean ALL data in the current keyspace and immediately commit."),
                is("Type 'confirm' to continue."),
                "no thanks bad idea thanks for warning me",
                is("Cancelling clean."),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity")
        );
    }

    @Test
    public void testExecuteMultipleQueries() throws Exception {
        assertShellMatches(
                "define X sub entity; insert $x isa X; match $y isa X; get; match $y isa X; aggregate count;",
                // Make sure we see results from all four queries
                containsString("{}"),
                containsString("$x"),
                containsString("$y"),
                is("1")
        );
    }

    @Test
    public void whenRunningBatchLoad_LoadCompletes() throws Exception {
        runShellWithoutErrors("", "-k", "batch", "-f", "src/test/graql/shell test(weird name).gql");
        runShellWithoutErrors("", "-k", "batch", "-b", "src/test/graql/batch-test.gql");

        assertShellMatches(ImmutableList.of("-k", "batch"),
                "match $x isa movie; aggregate ask;",
                containsString("True")
        );
    }

    @Test
    public void whenUserMakesAMistake_SubsequentQueriesStillWork() throws Exception {
        ShellResponse response = runShell(
                "match $x sub concet; aggregate count;\n" +
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; aggregate ask;\n"
        );

        assertThat(response.err(), not(containsString("error")));
        assertThat(response.out(), containsString("True"));
    }

    @Test
    public void whenUserMakesAMistake_SubsequentErrorsAreTheSame() throws Exception {
        String query = "insert r sub resource datatype string; e sub entity has r has nothing;";

        String err1 = runShell("", "-e", query).err();
        assertThat(err1, not(isEmptyString()));

        String err2 = runShell("", "-e", query).err();
        assertEquals(err1, err2);
    }

    @Test
    public void whenErrorOccurs_DoNotShowStackTrace() throws Exception {
        ShellResponse response = runShell("match fofobjiojasd\n");

        assertFalse(response.out(), response.err().isEmpty());
        assertThat(response.err(), not(containsString(".java")));
    }

    @Test
    public void whenErrorDoesNotOccurs_Return0() throws Exception {
        ShellResponse response = runShell("match $x sub entity; get;\n");
        assertTrue(response.success());
    }

    @Test
    public void whenErrorOccurs_Return1() throws Exception {
        ShellResponse response = runShell("match fofobjiojasd\n");
        assertFalse(response.success());
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

        String output = runShellWithoutErrors(input, arguments.toArray(new String[arguments.size()]));

        List<String> outputLines = Lists.newArrayList(output.replace(" \r", "").split("\n"));

        ImmutableSet<String> noPromptArgs = ImmutableSet.of("-e", "-f", "-b", "-v", "-h");
        if (Sets.intersection(Sets.newHashSet(arguments), noPromptArgs).isEmpty()) {
            // Remove first four lines containing license and last line containing prompt
            outputLines = outputLines.subList(4, outputLines.size() - 1);
        }

        assertThat(outputLines, contains(matcherList));
    }

    private String runShellWithoutErrors(String input, String... args) throws Exception {
        ShellResponse response = runShell(input, args);
        String errMessage = response.err();
        assertTrue("Error: \"" + errMessage + "\"", errMessage.isEmpty());
        return response.out();
    }

    private ShellResponse runShell(String input, String... args) throws Exception {
        args = specifyUniqueKeyspace(args);

        InputStream in = new ByteArrayInputStream(input.getBytes());

        OutputStream bout = new ByteArrayOutputStream();
        OutputStream berr = new ByteArrayOutputStream();

        OutputStream tout = bout;
        OutputStream terr = berr;

        if (showStdOutAndErr) {
            // Intercept stdout and stderr, but make sure it is still printed using the TeeOutputStream
            tout = new TeeOutputStream(bout, trueOut);
            terr = new TeeOutputStream(berr, trueErr);
        }

        PrintStream out = new PrintStream(tout);
        PrintStream err = new PrintStream(terr);

        Boolean success = null;

        try {
            System.out.flush();
            System.err.flush();
            System.setIn(in);
            System.setOut(out);
            System.setErr(err);

            success = GraqlShell.runShell(args, expectedVersion, historyFile);
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

        assertNotNull(success);

        return ShellResponse.of(bout.toString(), berr.toString(), success);
    }

    // TODO: Remove this when we can clear graphs properly (TP #13745)
    private String[] specifyUniqueKeyspace(String[] args) {
        List<String> argList = Lists.newArrayList(args);

        int keyspaceIndex = argList.indexOf("-k") + 1;
        if (keyspaceIndex == 0) {
            argList.add("-k");
            argList.add(GraqlShell.DEFAULT_KEYSPACE);
            keyspaceIndex = argList.size() - 1;
        }

        argList.set(keyspaceIndex, argList.get(keyspaceIndex) + keyspaceSuffix);

        return argList.toArray(new String[argList.size()]);
    }

    @AutoValue
    static abstract class ShellResponse {
        abstract String out();
        abstract String err();
        abstract boolean success();

        static ShellResponse of(String out, String err, boolean success) {
            return new AutoValue_GraqlShellIT_ShellResponse(out, err, success);
        }
    }
}

