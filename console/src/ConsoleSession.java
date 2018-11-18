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

package grakn.core.console;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.client.Grakn;
import grakn.core.graql.Query;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.internal.printer.Printer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
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

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.StringEscapeUtils.unescapeJavaScript;


/**
 * A Graql REPL shell that can be run from the command line
 *
 */
public class ConsoleSession implements AutoCloseable {

    private static final String EDIT = "edit";
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";
    private static final String LOAD = "load";
    private static final String DISPLAY = "display";
    private static final String CLEAR = "clear";
    private static final String EXIT = "exit";
    private static final String CLEAN = "clean";
    static final ImmutableList<String> COMMANDS = ImmutableList.of(EDIT, COMMIT, ROLLBACK, LOAD, DISPLAY, CLEAR, EXIT, CLEAN);

    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String HISTORY_FILENAME = StandardSystemProperty.USER_HOME.value() + "/.graql-history";

    private static final String COPYRIGHT = "\n" +
            "Welcome to Grakn Console. You are now in Grakn land!\n" +
            "Copyright (C) 2018  Grakn Labs Limited\n\n";

    private boolean errorOccurred = false;


    private final String keyspace;
    private final OutputFormat outputFormat;
    private final boolean infer;
    private ConsoleReader consoleReader;
    private final PrintStream serr;

    private final HistoryFile historyFile;

    private final Grakn client;
    private final Session session;
    private Transaction tx;
    private Set<AttributeType<?>> displayAttributes = ImmutableSet.of();

    private final GraqlCompleter graqlCompleter;
    private final ExternalEditor editor = ExternalEditor.create();


    static String loadQuery(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        return lines.stream().collect(joining("\n"));
    }

    ConsoleSession(String serverAddress, String keyspace, boolean infer, PrintStream printOut, PrintStream printErr) throws IOException {
        this.keyspace = keyspace;
        this.outputFormat = new OutputFormat.Graql();
        this.infer = infer;
        this.consoleReader = new ConsoleReader(System.in, printOut);
        this.client = new Grakn(serverAddress);
        this.session = client.session(keyspace);
        this.serr = printErr;
        this.graqlCompleter = GraqlCompleter.create(session);
        this.historyFile = HistoryFile.create(consoleReader, HISTORY_FILENAME);
    }

    public ConsoleSession start(@Nullable List<String> queryStrings) throws IOException, InterruptedException {
        tx = client.session(keyspace).transaction(Transaction.Type.WRITE);

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
            consoleReader.flush();
        }

        return this;
    }

    /**
     * The string to be displayed at the prompt
     */
    private static String consolePrompt(String keyspace) {
        return ANSI_PURPLE + keyspace + ANSI_RESET + "> ";
    }

    /**
     * Run a Read-Evaluate-Print loop until the input terminates
     */
    private void executeRepl() throws IOException, InterruptedException {
        consoleReader.print(COPYRIGHT);

        // Disable JLine feature when seeing a '!', which is used in our queries
        consoleReader.setExpandEvents(false);

        consoleReader.setPrompt(consolePrompt(tx.keyspace().getName()));

        // Add all autocompleters
        consoleReader.addCompleter(new AggregateCompleter(graqlCompleter, new ShellCommandCompleter()));

        String queryString;

        java.util.regex.Pattern commandPattern = java.util.regex.Pattern.compile("\\s*(.*?)\\s*;?");

        while ((queryString = consoleReader.readLine()) != null) {
            Matcher matcher = commandPattern.matcher(queryString);

            if (matcher.matches()) {
                switch (matcher.group(1)) {
                    case EDIT:
                        executeQuery(editor.execute());
                        continue;
                    case COMMIT:
                        commit();
                        continue;
                    case ROLLBACK:
                        rollback();
                        continue;
                    case CLEAN:
                        clean();
                        continue;
                    case CLEAR:
                        consoleReader.clearScreen();
                        continue;
                    case EXIT:
                        return;
                    case "":
                        // Ignore empty command
                        continue;
                }
            }

            // Load from a file if load command used
            if (queryString.startsWith(LOAD + " ")) {
                String pathString = queryString.substring(LOAD.length() + 1);
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
            if (queryString.startsWith(DISPLAY + " ")) {
                int endIndex;
                if (queryString.endsWith(";")) {
                    endIndex = queryString.length() - 1;
                } else {
                    endIndex = queryString.length();
                }
                String[] arguments = queryString.substring(DISPLAY.length() + 1, endIndex).split(",");
                Set<String> resources = Stream.of(arguments).map(String::trim).collect(toSet());
                setDisplayOptions(resources);
                continue;
            }

            executeQuery(queryString);
        }
    }

    private void executeQuery(String queryString) throws IOException {
        Printer<?> printer = outputFormat.getPrinter(displayAttributes);

        handleGraknExceptions(() -> {
            Stream<Query<Answer>> queries = tx
                    .graql()
                    .infer(infer)
                    .parser()
                    .parseList(queryString);

            Stream<String> results = queries.flatMap(query -> printer.toStream(query.stream()));

            results.forEach(result -> {
                try {
                    consoleReader.println(result);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });

        // Flush the ConsoleReader so the output is all displayed before the next command
        consoleReader.flush();
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
        consoleReader.println("Are you sure? This will clean ALL data in the current keyspace and immediately commit.");
        consoleReader.println("Type 'confirm' to continue...");
        String line = consoleReader.readLine();
        if (line != null && line.equals("confirm")) {
            consoleReader.print("Cleaning keyspace...");
            consoleReader.flush();
            client.keyspaces().delete(keyspace);
            consoleReader.println("done.");
            reopenTx();
        } else {
            consoleReader.println("Cancelling clean.");
        }
    }

    private void handleGraknExceptions(RunnableThrowsIO runnable) throws IOException {
        try {
            runnable.run();
        } catch (RuntimeException e) {
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
        tx = session.transaction(Transaction.Type.WRITE);
    }

    boolean errorOccurred() {
        return errorOccurred;
    }

    @Override
    public final void close() throws IOException {
        tx.close();
        session.close();
        historyFile.close();
    }
}
