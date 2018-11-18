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
import grakn.core.client.Grakn;
import grakn.core.graql.Query;
import grakn.core.graql.answer.Answer;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang.StringEscapeUtils.unescapeJavaScript;


/**
 * A Grakn Console Session that allows a user to interact with the Grakn Server
 */
public class ConsoleSession implements AutoCloseable {

    private static final String COPYRIGHT = "\n" +
            "Welcome to Grakn Console. You are now in Grakn land!\n" +
            "Copyright (C) 2018  Grakn Labs Limited\n\n";

    private static final String EDIT = "edit";
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";
    private static final String LOAD = "load";
    private static final String CLEAR = "clear";
    private static final String EXIT = "exit";
    private static final String CLEAN = "clean";
    static final ImmutableList<String> COMMANDS = ImmutableList.of(EDIT, COMMIT, ROLLBACK, LOAD, CLEAR, EXIT, CLEAN);

    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String HISTORY_FILENAME = StandardSystemProperty.USER_HOME.value() + "/.graql-history";

    private final boolean infer;
    private final String keyspace;
    private final PrintStream serr;
    private final ConsoleReader consoleReader;

    private final HistoryFile historyFile;
    private final GraqlCompleter graqlCompleter;
    private final ExternalEditor editor = ExternalEditor.create();

    private final Grakn client;
    private final Session session;
    private Transaction tx;

    private boolean hasError = false;

    static String loadQuery(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        return lines.stream().collect(joining("\n"));
    }

    ConsoleSession(String serverAddress, String keyspace, boolean infer, PrintStream printOut, PrintStream printErr) throws IOException {
        this.keyspace = keyspace;
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
                open();
            }
        } finally {
            consoleReader.flush();
        }

        return this;
    }

    private static String consolePrompt(String keyspace) {
        return ANSI_PURPLE + keyspace + ANSI_RESET + "> ";
    }

    private void open() throws IOException, InterruptedException {
        consoleReader.addCompleter(new AggregateCompleter(graqlCompleter, new ShellCommandCompleter()));
        consoleReader.setExpandEvents(false); // Disable JLine feature when seeing a '!'
        consoleReader.setPrompt(consolePrompt(session.keyspace().getName()));
        consoleReader.print(COPYRIGHT);

        String queryString;

        Pattern commandPattern = Pattern.compile("\\s*(.*?)\\s*;?");

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
                    hasError = true;
                    continue;
                }
            }

            executeQuery(queryString);
        }
    }

    private void executeQuery(String queryString) throws IOException {
        Printer<?> printer = Printer.stringPrinter(true);
        try {
            Stream<Query<Answer>> queries = tx.graql().infer(infer).parser().parseList(queryString);
            Stream<String> results = queries.flatMap(query -> printer.toStream(query.stream()));
            results.forEach(result -> {
                try {
                    consoleReader.println(result);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (RuntimeException e1) {
            serr.println(e1.getMessage());
            hasError = true;
            reopenTransaction();
        }

        consoleReader.flush(); // Flush the ConsoleReader before the next command
    }

    private void commit() {
        try {
            tx.commit();
        } catch (RuntimeException e) {
            serr.println(e.getMessage());
            hasError = true;
        } finally {
            reopenTransaction();
        }
    }

    private void rollback() {
        try {
            tx.close();
        } catch (RuntimeException e) {
            serr.println(e.getMessage());
            hasError = true;
        } finally {
            reopenTransaction();
        }
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
            reopenTransaction();
        } else {
            consoleReader.println("Clean command cancelled");
        }
    }

    private void reopenTransaction() {
        if (!tx.isClosed()) tx.close();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    boolean hasError() {
        return hasError;
    }

    @Override
    public final void close() throws IOException {
        tx.close();
        session.close();
        historyFile.close();
    }
}
