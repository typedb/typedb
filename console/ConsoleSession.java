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
import grakn.core.client.Grakn;
import grakn.core.graql.query.Query;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.printer.Printer;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.commons.lang.StringEscapeUtils.unescapeJava;


/**
 * A Grakn Console Session that allows a user to interact with the Grakn Server
 */
public class ConsoleSession implements AutoCloseable {

    private static final String COPYRIGHT = "\n" +
            "Welcome to Grakn Console. You are now in Grakn Wonderland!\n" +
            "Copyright (C) 2018 Grakn Labs Limited\n\n";

    private static final String EDITOR = "editor";
    private static final String COMMIT = "commit";
    private static final String ROLLBACK = "rollback";
    private static final String LOAD = "load";
    private static final String CLEAR = "clear";
    private static final String EXIT = "exit";
    private static final String CLEAN = "clean";

    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_RESET = "\u001B[0m";

    private static final String HISTORY_FILE = StandardSystemProperty.USER_HOME.value() + "/.grakn-console-history";
    private static final String EDITOR_DEFAULT = "vim";
    private static final String EDITOR_FILE = "/grakn-console-editor.gql";

    private final boolean infer;
    private final String keyspace;
    private final PrintStream printErr;
    private final ConsoleReader consoleReader;
    private final Printer<?> printer = Printer.stringPrinter(true);

    private final FileHistory historyFile;

    private final Grakn client;
    private final Grakn.Session session;
    private Grakn.Transaction tx;

    ConsoleSession(String serverAddress, String keyspace, boolean infer, PrintStream printOut, PrintStream printErr) throws IOException {
        this.keyspace = keyspace;
        this.infer = infer;
        this.client = new Grakn(serverAddress);
        this.session = client.session(keyspace);
        this.consoleReader = new ConsoleReader(System.in, printOut);
        this.consoleReader.setPrompt(ANSI_PURPLE + session.keyspace().getName() + ANSI_RESET + "> ");
        this.printErr = printErr;

        File file = new File(HISTORY_FILE);
        file.createNewFile();
        this.historyFile = new FileHistory(file);
        this.consoleReader.setHistory(this.historyFile);
    }

    void load(Path filePath) throws IOException {
        String queries = readFile(filePath);
        tx = client.session(keyspace).transaction(Grakn.Transaction.Type.WRITE);

        try {
            consoleReader.print("Loading: " + filePath.toString());
            consoleReader.println("...");
            executeQuery(queries);
            commit();
            consoleReader.println("...");
            consoleReader.println("Successful commit: " + filePath.toString());
        } finally {
            consoleReader.flush();
        }
    }

    void run() throws IOException, InterruptedException {
        consoleReader.setExpandEvents(false); // Disable JLine feature when seeing a '!'
        consoleReader.print(COPYRIGHT);

        tx = client.session(keyspace).transaction(Grakn.Transaction.Type.WRITE);
        String queryString;

        while ((queryString = consoleReader.readLine()) != null) {
            if (queryString.equals(EDITOR)) {
                executeQuery(openTextEditor());

            } else if (queryString.startsWith(LOAD + ' ')) {
                queryString = readFile(Paths.get(unescapeJava(queryString.substring(LOAD.length() + 1))));
                executeQuery(queryString);

            } else if (queryString.equals(COMMIT)) {
                commit();

            } else if (queryString.equals(ROLLBACK)) {
                rollback();

            } else if (queryString.equals(CLEAN)) {
                clean();

            } else if (queryString.equals(CLEAR)) {
                consoleReader.clearScreen();

            } else if (queryString.equals(EXIT)) {
                consoleReader.flush();
                return;

            } else if (!queryString.isEmpty()) {
                executeQuery(queryString);

            } // We ignore empty commands
        }
    }


    private static String readFile(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        return String.join("\n", lines);
    }

    private void executeQuery(String queryString) throws IOException {
        // We'll use streams so we can print the answer out much faster and smoother
        try {
            // Parse the string to get a stream of Graql Queries
            Stream<Query<Answer>> queries = tx.graql().parser().parseList(queryString);

            // Get the stream of answers for each query (query.stream())
            // Get the  stream of printed answers (printer.toStream(..))
            // Combine the stream of printed answers into one stream (queries.flatMap(..))
            Stream<String> answers = queries.flatMap(query -> printer.toStream(tx.stream(query, infer)));

            // For each printed answer, print them them on one line
            answers.forEach(answer -> {
                try {
                    consoleReader.println(answer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (RuntimeException e) {
            printErr.println(e.getMessage());
            reopenTransaction();
        }

        consoleReader.flush(); // Flush the ConsoleReader before the next command

        // It is important that we DO NOT close the transaction at the end of a query
        // The user may want to do consecutive operations onto the database
        // The transaction will only close once the user decides to COMMIT or ROLLBACK
    }

    private void commit() {
        try {
            tx.commit();
        } catch (RuntimeException e) {
            printErr.println(e.getMessage());
        } finally {
            reopenTransaction();
        }
    }

    private void rollback() {
        try {
            tx.close();
        } catch (RuntimeException e) {
            printErr.println(e.getMessage());
        } finally {
            reopenTransaction();
        }
    }

    private void clean() throws IOException {
        // Get user confirmation to clean graph
        consoleReader.println("Are you sure? CLEAN command will delete the current keyspace and its content.");
        consoleReader.println("Type 'confirm' to continue: ");

        String line = consoleReader.readLine();

        if (line != null && line.equals("confirm")) {
            consoleReader.println("Cleaning keyspace: " + keyspace);
            consoleReader.println("...");
            consoleReader.flush();
            client.keyspaces().delete(keyspace);
            consoleReader.println("Keyspace deleted: " + keyspace);

            reopenTransaction();

        } else {
            consoleReader.println("Clean command cancelled");
        }
    }

    private void reopenTransaction() {
        if (!tx.isClosed()) tx.close();
        tx = session.transaction(Grakn.Transaction.Type.WRITE);
    }

    @Override
    public final void close() throws IOException {
        tx.close();
        session.close();
        historyFile.flush();
    }

    /**
     * Open the user's preferred editor to write a query
     *
     * @return the string written in the editor
     */
    private String openTextEditor() throws IOException, InterruptedException {
        String editor = Optional.ofNullable(System.getenv().get("EDITOR")).orElse(EDITOR_DEFAULT);
        File tempFile = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value() + EDITOR_FILE);
        tempFile.createNewFile();

        // Run the editor, pipe input into and out of tty so we can provide the input/output to the editor via Graql
        ProcessBuilder builder = new ProcessBuilder(
                "/bin/bash", "-c",
                editor + " </dev/tty >/dev/tty " + tempFile.getAbsolutePath()
        );

        builder.start().waitFor();
        return String.join("\n", Files.readAllLines(tempFile.toPath()));
    }
}
