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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.shell;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.StringEscapeUtils.unescapeJavaScript;


/**
 * A Graql REPL shell that can be run from the command line
 *
 * @author Felix Chapman
 */
public class GraqlShell implements AutoCloseable {

    private static final String PROMPT = ">>> ";

    private static final String EDIT_COMMAND = "edit";
    private static final String COMMIT_COMMAND = "commit";
    private static final String ROLLBACK_COMMAND = "rollback";
    private static final String LOAD_COMMAND = "load";
    private static final String DISPLAY_COMMAND = "display";
    private static final String CLEAR_COMMAND = "clear";
    private static final String EXIT_COMMAND = "exit";
    private static final String LICENSE_COMMAND = "license";
    private static final String CLEAN_COMMAND = "clean";

    private boolean errorOccurred = false;

    /**
     * Array of available commands in shell
     */
    public static final ImmutableList<String> COMMANDS = ImmutableList.of(
            EDIT_COMMAND, COMMIT_COMMAND, ROLLBACK_COMMAND, LOAD_COMMAND, DISPLAY_COMMAND, CLEAR_COMMAND, EXIT_COMMAND,
            LICENSE_COMMAND, CLEAN_COMMAND
    );

    private final OutputFormat outputFormat;
    private final boolean infer;
    private ConsoleReader console;
    private final PrintStream serr;

    private final HistoryFile historyFile;

    private final GraknSession session;
    private GraknTx tx;
    private Set<AttributeType<?>> displayAttributes = ImmutableSet.of();

    private final GraqlCompleter graqlCompleter;
    private final ExternalEditor editor = ExternalEditor.create();


    public static String loadQuery(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        return lines.stream().collect(joining("\n"));
    }

    /**
     * Create a new Graql shell
     */
    public GraqlShell(
            String historyFilename, GraknSession session, ConsoleReader console, PrintStream serr,
            OutputFormat outputFormat, boolean infer

    ) throws IOException {

        this.outputFormat = outputFormat;
        this.infer = infer;
        this.console = console;
        this.session = session;
        this.serr = serr;
        this.graqlCompleter = GraqlCompleter.create(session);
        this.historyFile = HistoryFile.create(console, historyFilename);


        tx = session.open(GraknTxType.WRITE);
    }

    public void start(@Nullable List<String> queryStrings) throws IOException, InterruptedException {
        try {
            if (queryStrings != null) {
                for (String queryString : queryStrings) {
                    executeQuery(queryString);
                    commit();
                }
            } else {
                executeRepl();
            }
        } finally {
            console.flush();
        }
    }

    /**
     * Run a Read-Evaluate-Print loop until the input terminates
     */
    private void executeRepl() throws IOException, InterruptedException {
        License.printLicensePrompt(console);

        // Disable JLine feature when seeing a '!', which is used in our queries
        console.setExpandEvents(false);

        console.setPrompt(PROMPT);

        // Add all autocompleters
        console.addCompleter(new AggregateCompleter(graqlCompleter, new ShellCommandCompleter()));

        String queryString;

        java.util.regex.Pattern commandPattern = java.util.regex.Pattern.compile("\\s*(.*?)\\s*;?");

        while ((queryString = console.readLine()) != null) {
            Matcher matcher = commandPattern.matcher(queryString);

            if (matcher.matches()) {
                switch (matcher.group(1)) {
                    case EDIT_COMMAND:
                        executeQuery(editor.execute());
                        continue;
                    case COMMIT_COMMAND:
                        commit();
                        continue;
                    case ROLLBACK_COMMAND:
                        rollback();
                        continue;
                    case CLEAN_COMMAND:
                        clean();
                        continue;
                    case CLEAR_COMMAND:
                        console.clearScreen();
                        continue;
                    case LICENSE_COMMAND:
                        License.printLicense(console);
                        continue;
                    case EXIT_COMMAND:
                        return;
                    case "":
                        // Ignore empty command
                        continue;
                }
            }

            // Load from a file if load command used
            if (queryString.startsWith(LOAD_COMMAND + " ")) {
                String pathString = queryString.substring(LOAD_COMMAND.length() + 1);
                Path path = Paths.get(unescapeJavaScript(pathString));

                try {
                    queryString = loadQuery(path);
                } catch (IOException e) {
                    serr.println(e.toString());
                    errorOccurred = true;
                    continue;
                }
            }

            // Set the resources to display
            if (queryString.startsWith(DISPLAY_COMMAND + " ")) {
                int endIndex;
                if (queryString.endsWith(";")) {
                    endIndex = queryString.length() - 1;
                } else {
                    endIndex = queryString.length();
                }
                String[] arguments = queryString.substring(DISPLAY_COMMAND.length() + 1, endIndex).split(",");
                Set<String> resources = Stream.of(arguments).map(String::trim).collect(toSet());
                setDisplayOptions(resources);
                continue;
            }

            executeQuery(queryString);
        }
    }

    private void executeQuery(String queryString) throws IOException {
        GraqlConverter<?, String> converter = outputFormat.getConverter(displayAttributes);

        handleGraknExceptions(() -> {
            Stream<Query<?>> queries = tx
                    .graql()
                    .infer(infer)
                    .parser()
                    .parseList(queryString);

            Iterable<String> results = () -> queries
                    .flatMap(query -> query.results(converter))
                    .iterator();

            for (String result : results) {
                console.println(result);
            }
        });

        // Flush the console so the output is all displayed before the next command
        console.flush();
    }

    private void setDisplayOptions(Set<String> displayOptions) {
        displayAttributes = displayOptions.stream().map(tx::getAttributeType).collect(toImmutableSet());
    }

    private void commit() throws IOException {
        handleGraknExceptions(() -> tx.commit());
        reopenTx();
    }

    private void rollback() throws IOException {
        handleGraknExceptions(() -> tx.close());
        reopenTx();
    }

    private void clean() throws IOException {
        // Get user confirmation to clean graph
        console.println("Are you sure? This will clean ALL data in the current keyspace and immediately commit.");
        console.println("Type 'confirm' to continue.");
        String line = console.readLine();
        if (line != null && line.equals("confirm")) {
            console.println("Cleaning...");
            tx.admin().delete();
            reopenTx();
        } else {
            console.println("Cancelling clean.");
        }
    }

    private void handleGraknExceptions(RunnableThrowsIO runnable) throws IOException {
        try {
            runnable.run();
        } catch (GraknException e) {
            serr.println(e.getMessage());
            errorOccurred = true;
            reopenTx();
        }
    }

    private interface RunnableThrowsIO {
        void run() throws IOException;
    }

    private void reopenTx() {
        if (!tx.isClosed()) tx.close();
        tx = session.open(GraknTxType.WRITE);
    }

    public boolean errorOccurred() {
        return errorOccurred;
    }

    @Override
    public final void close() throws IOException {
        tx.close();
        session.close();
        historyFile.close();
    }
}
