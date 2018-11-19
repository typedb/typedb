/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.console.test;

import com.google.auto.value.AutoValue;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.common.util.GraknVersion;
import grakn.core.console.GraknConsole;
import grakn.core.graql.internal.Schema;
import grakn.core.rule.GraknTestServer;
import io.grpc.Status;
import org.apache.commons.io.output.TeeOutputStream;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GraknConsoleIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static InputStream trueIn;
    private static int keyspaceSuffix = 0;

    private final static String analyticsDataset = "define obj sub entity, plays rel; relation sub relationship, relates rel; " +
            "insert $a isa obj; $b isa obj; $c isa obj; $d isa obj; " +
            "(rel: $a, rel: $b) isa relation; (rel: $a, rel: $c) isa relation; (rel: $a, rel: $d) isa relation; ";

    @BeforeClass
    public static void setUpClass() throws IOException {
        trueIn = System.in;
        System.setProperty(
                StandardSystemProperty.USER_HOME.key(),
                Files.createTempDirectory("grakn-console").toString()
        );
    }

    @Before
    public void changeSuffix() {
        keyspaceSuffix += 1;
    }

    @AfterClass
    public static void resetIO() {
        System.setIn(trueIn);
    }

    @Test
    public void when_consoleSessionExitCommand_expect_consoleTerminates() {
        // Assert simply that the shell starts and terminates without errors
        String output = runConsoleSessionWithoutExpectingErrors("exit\n");
        assertTrue(output.matches("[\\s\\S]*> exit(\r\n?|\n)"));
    }

    @Test
    public void when_startingConsoleWithOptionHelp_expect_printHelpMessage() {
        String result = runConsoleSessionWithoutExpectingErrors("", "--help");

        // Check for a few expected usage messages
        assertThat(
                result,
                allOf(
                        containsString("usage"), containsString("grakn console [options]"),
                        containsString("-f"), containsString("--file <arg>"), containsString("path to a Graql file")
                )
        );
    }

    @Test
    public void when_startingConsoleWithOptionVersion_expect_printVersion() {
        String result = runConsoleSessionWithoutExpectingErrors("", "--version");
        assertThat(result, containsString(GraknVersion.VERSION));
    }

    @Test
    public void when_writingToDefaultKeyspace_expect_successReadFromDefaultKeyspace() throws Exception {
        runConsoleSessionWithoutExpectingErrors("define im-in-the-default-keyspace sub entity;\ncommit\n");

        assertConsoleSessionMatches(
                ImmutableList.of("-k", "grakn"),
                "match im-in-the-default-keyspace sub entity; aggregate count;",
                containsString("1")
        );
    }

    @Test
    public void when_writingToDifferentKeyspaces_expect_theyDoNotGetMixedUp() {
        runConsoleSessionWithoutExpectingErrors("define foo-foo sub entity;\ncommit\n", "-k", "foo");
        runConsoleSessionWithoutExpectingErrors("define bar-bar sub entity;\ncommit\n", "-k", "bar");

        String fooFooInFoo = runConsoleSessionWithoutExpectingErrors("match foo-foo sub entity; aggregate count;\n", "-k", "foo");
        String fooFooInBar = runConsoleSessionWithoutExpectingErrors("match foo-foo sub entity; aggregate count;\n", "-k", "bar");
        String barBarInFoo = runConsoleSessionWithoutExpectingErrors("match bar-bar sub entity; aggregate count;\n", "-k", "foo");
        String barBarInBar = runConsoleSessionWithoutExpectingErrors("match bar-bar sub entity; aggregate count;\n", "-k", "bar");
        assertThat(fooFooInFoo, containsString("1"));
        assertThat(fooFooInBar, containsString("0"));
        assertThat(barBarInFoo, containsString("0"));
        assertThat(barBarInBar, containsString("1"));
    }

    @Test
    public void when_startingConsoleWithOptionLoadFile_expect_noError() {
        Response response = runConsoleSession("", "-f", "console/test/file-(with-parentheses).gql");
        assertEquals("", response.err());
    }

    @Test
    public void when_loadingFileInConsoleSession_expect_dataIsWritten() throws Exception {
        assertConsoleSessionMatches(
                "load console/test/file-(with-parentheses).gql",
                anything(),
                "match movie sub entity; aggregate count;",
                containsString("1")
        );
    }

    @Test
    public void when_loadingFileWithEscapes_expect_dataIsWritten() throws Exception {
        assertConsoleSessionMatches(
                "load console/test/file-\\(with-parentheses\\).gql",
                anything(),
                "match movie sub entity; aggregate count;",
                containsString("1")
        );
    }

    @Test
    public void when_writingMatchQueries_expect_resultsReturned() {
        String[] result = runConsoleSessionWithoutExpectingErrors(
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;\nexit"
        ).split("\r\n?|\n");

        // Make sure we find a few results (don't be too fussy about the output here)
        assertThat(result[4], endsWith("> match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; get;"));
        assertTrue(result.length > 5);
    }

    @Test
    public void when_writingRelationships_expect_dataIsWritten() throws Exception {
        assertConsoleSessionMatches(
                "define name sub attribute datatype string;",
                anything(),
                "define marriage sub relationship, relates spouse;",
                anything(),
                "define person sub entity, has name, plays spouse;",
                anything(),
                "insert isa person has name \"Bill Gates\";",
                anything(),
                "insert isa person has name \"Melinda Gates\";",
                anything(),
                "match $husband isa person has name \"Bill Gates\"; $wife isa person has name \"Melinda Gates\"; insert (spouse: $husband, spouse: $wife) isa marriage;",
                anything(),
                "match $x isa marriage; get;",
                allOf(containsString("spouse"), containsString("isa"), containsString("marriage"))
        );
    }

    @Test
    public void when_writingInsertQueries_expect_dataIsWritten() throws Exception {
        assertConsoleSessionMatches(
                "define entity2 sub entity;",
                anything(),
                "match $x isa entity2; aggregate count $x;",
                containsString("0"),
                "insert $x isa entity2;",
                anything(),
                "match $x isa entity2; aggregate count $x;",
                containsString("1")
        );
    }

    @Test
    public void when_writingInsertQueries_expect_writtenDataPrinted() throws Exception {
        assertConsoleSessionMatches(
                "define X sub entity; insert $thingy isa X;",
                containsString("{}"),
                allOf(containsString("$thingy"), containsString("isa"), containsString("X"))
        );
    }

    @Test
    public void when_writingAggregateCountQuery_expect_correctCount() throws Exception {
        int NUM_METATYPES = 4;
        assertConsoleSessionMatches(
                "match $x sub " + Schema.MetaSchema.THING.getLabel().getValue() + "; aggregate count;",
                is(Integer.toString(NUM_METATYPES))
        );
    }

    @Test
    public void when_writingAggregateGroupQuery_expect_correctGroupingOfAnswers() throws Exception {
        assertConsoleSessionMatches(
                "define name sub attribute, datatype string;",
                anything(),
                "define person sub entity, has name;",
                anything(),
                "insert $x isa person, has name \"Alice\";",
                anything(),
                "insert $x isa person, has name \"Bob\";",
                anything(),
                "match $x isa person, has name $y; aggregate group $x;",
                anyOf(containsString("Alice"), containsString("Bob")),
                anyOf(containsString("Alice"), containsString("Bob"))
        );
    }

    @Test
    public void when_startingConsoleWithOptionNoInfer_expect_queriesDoNotInfer() throws Exception {
        assertConsoleSessionMatches(
                ImmutableList.of("--no_infer"),
                "define man sub entity has name; name sub attribute datatype string;",
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
    public void when_startingConsoleWithoutOptionNoInfer_expect_queriesToInfer() throws Exception {
        assertConsoleSessionMatches(
                "define man sub entity has name; name sub attribute datatype string;",
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
    public void when_writingInvalidQuery_expect_errorsInResponse() {
        Response response = runConsoleSession(
                "define movie sub entity; insert $moon isa movie; $europa isa $moon;\n"
        );

        assertThat(response.err(), allOf(containsString("not"), containsString("type")));
    }

    @Test
    public void when_writingComputeQueries_expect_correctCount() throws Exception {
        assertConsoleSessionMatches(
                analyticsDataset,
                anything(),
                anything(),
                "commit",
                "compute count;",
                is("7")
        );
    }

    @Test
    public void when_rollback_expect_transactionIsCancelled() {
        String[] result = runConsoleSessionWithoutExpectingErrors("define E sub entity;\nrollback\nmatch $x label E; get;\n").split("\n");

        // Make sure there are no results for get query
        assertThat(result[result.length - 2], endsWith("> match $x label E; get;"));
        assertThat(result[result.length - 1], endsWith("> "));
    }

    @Test
    public void when_writingQueryWithLimitOne_expect_oneLineResponse() throws Exception {
        assertConsoleSessionMatches(
                "match $x sub thing; limit 1; get;",
                anything() // Only one result
        );
    }

    @Test
    public void when_serverIsNotRunning_expect_connectionError() {
        Response response = runConsoleSession("", "-r", "localhost:7654");
        assertThat(response.err(), containsString(Status.Code.UNAVAILABLE.name()));
    }

    @Test
    public void when_commingInvalidData_expect_commitError() {
        Response response = runConsoleSession("insert bob sub relation;\ncommit");
        assertFalse(response.out(), response.err().isEmpty());
    }

    @Test
    public void when_runningCleanCommand_expect_keyspaceIsDeleted() throws Exception {
        assertConsoleSessionMatches(
                "define my-type sub entity;",
                is("{}"),
                "commit",
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                containsString("Are you sure?"),
                containsString("Type 'confirm' to continue"),
                "confirm",
                containsString("Cleaning keyspace"),
                anything(),
                containsString("Keyspace deleted"),
                "match $x sub entity; get;",
                containsString("entity"),
                "rollback",
                "match $x sub entity; get;",
                containsString("entity")
        );
    }

    @Test
    public void when_cancellingCleanCommand_expect_keyspaceIsNotDeleted() throws Exception {
        assertConsoleSessionMatches(
                "define my-type sub entity;",
                is("{}"),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                containsString("Are you sure?"),
                containsString("Type 'confirm' to continue"),
                "n",
                is("Clean command cancelled"),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity"),
                "clean",
                containsString("Are you sure?"),
                containsString("Type 'confirm' to continue"),
                "no thanks bad idea thanks for warning me",
                is("Clean command cancelled"),
                "match $x sub entity; get;",
                containsString("entity"),
                containsString("entity")
        );
    }

    @Test
    public void when_writingMultipleQueries_expect_multipleResponses() throws Exception {
        assertConsoleSessionMatches(
                "define X sub entity; insert $x isa X; match $y isa X; get; match $y isa X; aggregate count;",
                // Make sure we see results from all four queries
                containsString("{}"),
                containsString("$x"),
                containsString("$y"),
                is("1")
        );
    }

    @Test
    public void when_writingMultipleQueriesWithError_expect_multipleResponsesWithError() {
        Response response = runConsoleSession(
                "match $x sub somerandomstring; aggregate count;\n" +
                        "match $x sub thing; aggregate count;\n"
        );

        assertThat(response.err(), not(containsString("error")));
        assertThat(response.out(), containsString("1"));
    }

    @Test
    public void when_ErrorOccurs_expect_noStackTrace() {
        Response response = runConsoleSession("match fofobjiojasd\n");

        assertFalse(response.out(), response.err().isEmpty());
        assertThat(response.err(), not(containsString(".java")));
    }

    private void assertConsoleSessionMatches(Object... matchers) throws Exception {
        assertConsoleSessionMatches(ImmutableList.of(), matchers);
    }

    // Arguments should be strings or matchers. Strings are interpreted as input, matchers as expected output
    private void assertConsoleSessionMatches(List<String> arguments, Object... matchers) throws Exception {
        String input = Stream.of(matchers)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .collect(joining("\n", "", "\n"));

        List<Matcher<? super String>> matcherList = Stream.of(matchers)
                .map(obj -> (obj instanceof Matcher) ? (Matcher<String>) obj : endsWith("> " + obj))
                .collect(toList());

        String output = runConsoleSessionWithoutExpectingErrors(input, arguments.toArray(new String[arguments.size()]));

        List<String> outputLines = Lists.newArrayList(output.replace(" \r", "").split("\n"));

        ImmutableSet<String> noPromptArgs = ImmutableSet.of("-e", "-f", "-b", "-v", "-h");
        if (Sets.intersection(Sets.newHashSet(arguments), noPromptArgs).isEmpty()) {
            // Remove first four lines containing license and last line containing prompt
            outputLines = outputLines.subList(4, outputLines.size() - 1);
        }

        assertThat(outputLines, contains(matcherList));
    }

    private String runConsoleSessionWithoutExpectingErrors(String input, String... args) {
        Response response = runConsoleSession(input, args);
        String errMessage = response.err();
        assertTrue("Error: \"" + errMessage + "\"", errMessage.isEmpty());
        return response.out();
    }

    private Response runConsoleSession(String input, String... args) {
        args = addKeyspaceAndUriParams(args);

        OutputStream bufferOut = new ByteArrayOutputStream();
        OutputStream bufferErr = new ByteArrayOutputStream();

        PrintStream printOut = new PrintStream(new TeeOutputStream(bufferOut, System.out));
        PrintStream printErr = new PrintStream(new TeeOutputStream(bufferErr, System.err));

        try {
            System.setIn(new ByteArrayInputStream(input.getBytes()));
            GraknConsole console = new GraknConsole(args, printOut, printErr);
            console.run();
        } catch (Exception e) {
            System.out.println("We failed. Here's the stacktrace:");
            printErr.println(e.getMessage());
            printErr.flush();
        } finally {
            resetIO();
        }

        printOut.flush();
        printErr.flush();

        return Response.of(bufferOut.toString(), bufferErr.toString());
    }

    private String[] addKeyspaceAndUriParams(String[] args) {
        List<String> argList = Lists.newArrayList(args);

        int keyspaceIndex = argList.indexOf("-k") + 1;
        if (keyspaceIndex == 0) {
            argList.add("-k");
            argList.add(GraknConsole.DEFAULT_KEYSPACE);
            keyspaceIndex = argList.size() - 1;
        }

        argList.set(keyspaceIndex, argList.get(keyspaceIndex) + keyspaceSuffix);

        boolean uriSpecified = argList.contains("-r");
        if (!uriSpecified) {
            argList.add("-r");
            argList.add(server.grpcUri().toString());
        }

        return argList.toArray(new String[argList.size()]);
    }

    @AutoValue
    static abstract class Response {
        abstract String out();

        abstract String err();

        static Response of(String out, String err) {
            return new AutoValue_GraknConsoleIT_Response(out, err);
        }
    }
}
