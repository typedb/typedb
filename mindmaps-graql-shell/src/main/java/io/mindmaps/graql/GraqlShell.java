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

package io.mindmaps.graql;

import io.mindmaps.graql.internal.shell.Version;
import io.mindmaps.graql.internal.shell.ErrorMessage;
import io.mindmaps.graql.internal.shell.GraQLCompleter;
import io.mindmaps.graql.internal.shell.GraqlSignalHandler;
import io.mindmaps.graql.internal.shell.ShellCommandCompleter;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.history.FileHistory;
import mjson.Json;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import sun.misc.Signal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION_AUTOCOMPLETE;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION_COMMIT;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION_NAMESPACE;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION_QUERY;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ACTION_QUERY_END;
import static io.mindmaps.constants.RESTUtil.RemoteShell.AUTOCOMPLETE_CURSOR;
import static io.mindmaps.constants.RESTUtil.RemoteShell.ERROR;
import static io.mindmaps.constants.RESTUtil.RemoteShell.NAMESPACE;
import static io.mindmaps.constants.RESTUtil.RemoteShell.QUERY;
import static io.mindmaps.constants.RESTUtil.RemoteShell.QUERY_LINES;
import static io.mindmaps.constants.RESTUtil.WebPath.REMOTE_SHELL_URI;

/**
 * A Graql REPL shell that can be run from the command line
 */
@WebSocket
public class GraqlShell implements AutoCloseable {
    private static final String LICENSE_PROMPT = "\n" +
            "MindmapsDB  Copyright (C) 2016  Mindmaps Research Ltd \n" +
            "This is free software, and you are welcome to redistribute it \n" +
            "under certain conditions; type 'license' for details.\n";

    private static final String LICENSE_LOCATION = "LICENSE.txt";

    private static final String DEFAULT_NAMESPACE = "mindmaps";
    private static final String DEFAULT_URI = "localhost:4567";

    private static final String PROMPT = ">>> ";

    private static final String EDIT_COMMAND = "edit";
    private static final String COMMIT_COMMAND = "commit";
    private static final String LOAD_COMMAND = "load";
    private static final String CLEAR_COMMAND = "clear";
    private static final String EXIT_COMMAND = "exit";
    private static final String LICENSE_COMMAND = "license";

    /**
     * Array of available commands in shell
     */
    public static final String[] COMMANDS = {EDIT_COMMAND, COMMIT_COMMAND, LOAD_COMMAND, CLEAR_COMMAND, EXIT_COMMAND};

    private static final String TEMP_FILENAME = "/graql-tmp.gql";
    private static final String HISTORY_FILENAME = "/graql-history";

    private static final String DEFAULT_EDITOR = "vim";

    private final File tempFile = new File(System.getProperty("java.io.tmpdir") + TEMP_FILENAME);
    private final String namespace;
    private ConsoleReader console;

    // A future containing the session, once the client has connected
    private CompletableFuture<Session> session = new CompletableFuture<>();

    // A future containing an autocomplete result, once it has been received
    private CompletableFuture<Json> autocompleteResponse = new CompletableFuture<>();

    private boolean waitingQuery = false;

    /**
     * Run a Graql REPL
     * @param args arguments to the Graql shell. Possible arguments can be listed by running {@code graql.sh --help}
     */
    public static void main(String[] args) {
        runShell(args, Version.VERSION, new GraqlClientImpl());
    }

    public static void runShell(String[] args, String version, GraqlClient client) {

        Options options = new Options();
        options.addOption("n", "name", true, "name of the graph");
        options.addOption("e", "execute", true, "query to execute");
        options.addOption("f", "file", true, "graql file path to execute");
        options.addOption("u", "uri", true, "uri to connect to engine");
        options.addOption("h", "help", false, "print usage message");
        options.addOption("v", "version", false, "print version");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return;
        }

        String query = cmd.getOptionValue("e");
        String filePath = cmd.getOptionValue("f");

        // Print usage message if requested or if invalid arguments provided
        if (cmd.hasOption("h") || !cmd.getArgList().isEmpty()) {
            HelpFormatter helpFormatter = new HelpFormatter();
            PrintWriter printWriter = new PrintWriter(System.out);
            int width = helpFormatter.getWidth();
            int leftPadding = helpFormatter.getLeftPadding();
            int descPadding = helpFormatter.getDescPadding();
            helpFormatter.printHelp(printWriter, width, "graql.sh", null, options, leftPadding, descPadding, null);
            printWriter.flush();
            return;
        }

        if (cmd.hasOption("v")) {
            System.out.println(version);
            return;
        }

        String namespace = cmd.getOptionValue("n", DEFAULT_NAMESPACE);
        String uriString = cmd.getOptionValue("u", DEFAULT_URI);

        try(GraqlShell shell = new GraqlShell(namespace)) {
            client.connect(shell, new URI("ws://" + uriString + REMOTE_SHELL_URI));

            if (filePath != null) {
                query = loadQuery(filePath);
            }

            if (query != null) {
                shell.executeQuery(query);
                shell.commit();
            } else {
                shell.executeRepl();
            }
        } catch (IOException | InterruptedException | ExecutionException | URISyntaxException e) {
            System.err.println(e.toString());
        }
    }

    private static String loadQuery(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
        return lines.stream().collect(Collectors.joining("\n"));
    }

    /**
     * Create a new Graql shell
     */
    GraqlShell(String namespace) throws IOException {
        this.namespace = namespace;
        console = new ConsoleReader(System.in, System.out);

        // Create handler to handle SIGINT (Ctrl-C) interrupts
        Signal signal = new Signal("INT");
        GraqlSignalHandler signalHandler = new GraqlSignalHandler(this);
        Signal.handle(signal, signalHandler);
    }

    @Override
    public void close() throws IOException, ExecutionException, InterruptedException {
        console.flush();
        session.get().close();
    }

    /**
     * Run a Read-Evaluate-Print loop until the input terminates
     */
    void executeRepl() throws IOException {
        console.print(LICENSE_PROMPT);

        // Disable JLine feature when seeing a '!', which is used in our queries
        console.setExpandEvents(false);

        console.setPrompt(PROMPT);

        // Create temporary file
        if (!tempFile.exists()) {
            boolean success = tempFile.createNewFile();
            if (!success) print(ErrorMessage.COULD_NOT_CREATE_TEMP_FILE.getMessage());
        }

        // Create history file
        File historyFile = new File(System.getProperty("java.io.tmpdir") + HISTORY_FILENAME);
        //noinspection ResultOfMethodCallIgnored
        historyFile.createNewFile();
        FileHistory history = new FileHistory(historyFile);
        console.setHistory(history);

        // Add all autocompleters
        console.addCompleter(new AggregateCompleter(new GraQLCompleter(this), new ShellCommandCompleter()));

        String queryString;

        while ((queryString = console.readLine()) != null) {
            history.flush();

            switch (queryString) {
                case EDIT_COMMAND:
                    executeQuery(runEditor());
                    break;
                case COMMIT_COMMAND:
                    commit();
                    break;
                case CLEAR_COMMAND:
                    console.clearScreen();
                    break;
                case LICENSE_COMMAND:
                    printLicense();
                    break;
                case EXIT_COMMAND:
                    return;
                case "":
                    // Ignore empty command
                    break;
                default:
                    // Load from a file if load command used
                    if (queryString.startsWith(LOAD_COMMAND + " ")) {
                        String path = queryString.substring(LOAD_COMMAND.length() + 1);

                        try {
                            queryString = loadQuery(path);
                        } catch (IOException e) {
                            System.err.println(e.toString());
                            break;
                        }
                    }

                    executeQuery(queryString);
                    break;
            }
        }
    }

    private void printLicense(){
        StringBuilder result = new StringBuilder("");

        //Get file from resources folder
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(LICENSE_LOCATION);

        Scanner scanner = new Scanner(is);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.append(line).append("\n");
        }
        result.append("\n");
        scanner.close();

        this.print(result.toString());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) throws IOException, ExecutionException, InterruptedException {
        // Send the requested keyspace to the server once connected
        sendJson(Json.object(ACTION, ACTION_NAMESPACE, NAMESPACE, namespace), session);
        this.session.complete(session);
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        Json json = Json.read(msg);

        if (json.has(ERROR)) {
            System.err.println(json.at(ERROR).asString());
        }

        switch (json.at(ACTION).asString()) {
            case ACTION_QUERY:
                List<Json> lines = json.at(QUERY_LINES).asJsonList();
                lines.forEach(line -> println(line.asString()));
                break;
            case ACTION_QUERY_END:
                // Alert the shell that the query has finished, so it can prompt for another query
                synchronized (this) {
                    notifyAll();
                }
                break;
            case ACTION_AUTOCOMPLETE:
                autocompleteResponse.complete(json);
                break;
        }
    }

    private void executeQuery(String queryString) {
        try {
            sendJson(Json.object(
                    ACTION, ACTION_QUERY,
                    QUERY, queryString
            ));

            // Wait for the end of the query results before continuing
            waitingQuery = true;
            synchronized (this) {
                wait();
            }
            waitingQuery = false;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void commit() {
        sendJson(Json.object(ACTION, ACTION_COMMIT));
    }

    /**
     * load the user's preferred editor to edit a query
     * @return the string written to the editor
     */
    private String runEditor() throws IOException {
        // Get preferred editor
        Map<String, String> env = System.getenv();
        String editor = Optional.ofNullable(env.get("EDITOR")).orElse(DEFAULT_EDITOR);

        // Run the editor, pipe input into and out of tty so we can provide the input/output to the editor via Graql
        ProcessBuilder builder = new ProcessBuilder(
                "/bin/bash",
                "-c",
                editor + " </dev/tty >/dev/tty " + tempFile.getAbsolutePath()
        );

        // Wait for user to finish editing
        try {
            builder.start().waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return String.join("\n", Files.readAllLines(tempFile.toPath()));
    }

    /**
     * Interrupt the shell. If the user is waiting for query results, tell the server to stop sending them.
     * Otherwise, exit normally.
     */
    public void interrupt() {
        if (waitingQuery) {
            sendJson(Json.object(ACTION, ACTION_QUERY_END));
            waitingQuery = false;
        } else {
            System.exit(0);
        }
    }

    public synchronized Json getAutocompleteCandidates(
            String queryString, int cursorPosition
    ) throws InterruptedException, ExecutionException, IOException {
        sendJson(Json.object(
                ACTION, ACTION_AUTOCOMPLETE,
                QUERY, queryString,
                AUTOCOMPLETE_CURSOR, cursorPosition
        ));

        Json json = autocompleteResponse.get();

        autocompleteResponse = new CompletableFuture<>();

        return json;
    }

    private void sendJson(Json json) {
        try {
            sendJson(json, session.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendJson(Json json, Session session) {
        try {
            session.getRemote().sendString(json.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void print(String string) {
        try {
            console.print(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void println(String string) {
        print(string + "\n");
    }
}
