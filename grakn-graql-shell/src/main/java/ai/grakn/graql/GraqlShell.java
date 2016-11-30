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

package ai.grakn.graql;

import ai.grakn.graql.internal.shell.ErrorMessage;
import ai.grakn.graql.internal.shell.GraqlCompleter;
import ai.grakn.graql.internal.shell.GraqlSignalHandler;
import ai.grakn.graql.internal.shell.ShellCommandCompleter;
import ai.grakn.util.GraknVersion;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
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
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import sun.misc.Signal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.REST.RemoteShell.ACTION;
import static ai.grakn.util.REST.RemoteShell.ACTION_COMMIT;
import static ai.grakn.util.REST.RemoteShell.ACTION_DISPLAY;
import static ai.grakn.util.REST.RemoteShell.ACTION_END;
import static ai.grakn.util.REST.RemoteShell.ACTION_ERROR;
import static ai.grakn.util.REST.RemoteShell.ACTION_INIT;
import static ai.grakn.util.REST.RemoteShell.ACTION_PING;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY;
import static ai.grakn.util.REST.RemoteShell.ACTION_QUERY_ABORT;
import static ai.grakn.util.REST.RemoteShell.ACTION_ROLLBACK;
import static ai.grakn.util.REST.RemoteShell.ACTION_TYPES;
import static ai.grakn.util.REST.RemoteShell.DISPLAY;
import static ai.grakn.util.REST.RemoteShell.ERROR;
import static ai.grakn.util.REST.RemoteShell.KEYSPACE;
import static ai.grakn.util.REST.RemoteShell.OUTPUT_FORMAT;
import static ai.grakn.util.REST.RemoteShell.QUERY;
import static ai.grakn.util.REST.RemoteShell.QUERY_RESULT;
import static ai.grakn.util.REST.RemoteShell.TYPES;
import static ai.grakn.util.REST.WebPath.IMPORT_DATA_URI;
import static ai.grakn.util.REST.WebPath.REMOTE_SHELL_URI;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.StringEscapeUtils.escapeJavaScript;

/**
 * A Graql REPL shell that can be run from the command line
 */
@WebSocket
public class GraqlShell {
    private static final String LICENSE_PROMPT = "\n" +
            "Grakn  Copyright (C) 2016  Grakn Labs Limited \n" +
            "This is free software, and you are welcome to redistribute it \n" +
            "under certain conditions; type 'license' for details.\n";

    private static final String LICENSE_LOCATION = "LICENSE.txt";

    private static final String DEFAULT_KEYSPACE = "grakn";
    private static final String DEFAULT_URI = "localhost:4567";
    private static final String DEFAULT_OUTPUT_FORMAT = "graql";

    private static final String PROMPT = ">>> ";

    private static final String EDIT_COMMAND = "edit";
    private static final String COMMIT_COMMAND = "commit";
    private static final String ROLLBACK_COMMAND = "rollback";
    private static final String LOAD_COMMAND = "load";
    private static final String DISPLAY_COMMAND = "display";
    private static final String CLEAR_COMMAND = "clear";
    private static final String EXIT_COMMAND = "exit";
    private static final String LICENSE_COMMAND = "license";

    private static final int QUERY_CHUNK_SIZE = 1000;
    private static final int PING_INTERVAL = 60_000;

    /**
     * Array of available commands in shell
     */
    public static final String[] COMMANDS = {
        EDIT_COMMAND, ROLLBACK_COMMAND, COMMIT_COMMAND, LOAD_COMMAND, CLEAR_COMMAND, EXIT_COMMAND
    };

    private static final String TEMP_FILENAME = "/graql-tmp.gql";
    private static final String HISTORY_FILENAME = "/graql-history";

    private static final String DEFAULT_EDITOR = "vim";

    private final File tempFile = new File(System.getProperty("java.io.tmpdir") + TEMP_FILENAME);
    private ConsoleReader console;

    private final String historyFilename;

    private Session session;

    private boolean waitingQuery = false;
    private final GraqlCompleter graqlCompleter = new GraqlCompleter();

    /**
     * Run a Graql REPL
     * @param args arguments to the Graql shell. Possible arguments can be listed by running {@code graql.sh --help}
     */
    public static void main(String[] args) {
        runShell(args, GraknVersion.VERSION, HISTORY_FILENAME, new GraqlClientImpl());
    }

    public static void runShell(String[] args, String version, String historyFilename, GraqlClient client) {

        Options options = new Options();
        options.addOption("k", "keyspace", true, "keyspace of the graph");
        options.addOption("e", "execute", true, "query to execute");
        options.addOption("f", "file", true, "graql file path to execute");
        options.addOption("u", "uri", true, "uri to factory to engine");
        options.addOption("b", "batch", true, "graql file path to batch load");
        options.addOption("o", "output", true, "output format for results");
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

        Optional<List<String>> queries = Optional.ofNullable(cmd.getOptionValue("e")).map(Lists::newArrayList);
        String[] filePaths = cmd.getOptionValues("f");

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

        String keyspace = cmd.getOptionValue("k", DEFAULT_KEYSPACE);
        String uriString = cmd.getOptionValue("u", DEFAULT_URI);
        String outputFormat = cmd.getOptionValue("o", DEFAULT_OUTPUT_FORMAT);

        if (cmd.hasOption("b")) {
            try {
                sendBatchRequest(uriString, cmd.getOptionValue("b"), keyspace);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }


        try {
            if (filePaths != null) {
                queries = Optional.of(loadQueries(filePaths));
            }

            URI uri = new URI("ws://" + uriString + REMOTE_SHELL_URI);

            new GraqlShell(historyFilename, keyspace, client, uri, queries, outputFormat);
        } catch (java.net.ConnectException e) {
            System.err.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
        } catch (Throwable e) {
            System.err.println(e.toString());
        }
    }

    private static List<String> loadQueries(String[] filePaths) throws IOException {
        List<String> queries = Lists.newArrayList();

        for (String filePath : filePaths) {
            queries.add(loadQuery(filePath));
        }

        return queries;
    }

    private static String loadQuery(String filePath) throws IOException {
            List<String> lines = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
            return lines.stream().collect(Collectors.joining("\n"));
    }

    private static void sendBatchRequest(String uriString, String graqlPath, String keyspace) throws IOException {
        byte[] out = ("{\"path\": \"" + escapeJavaScript(graqlPath) + "\"}").getBytes(StandardCharsets.UTF_8);

        URL url = new URL("http://" + uriString + IMPORT_DATA_URI + "?keyspace=" + keyspace);
        URLConnection con = url.openConnection();
        HttpURLConnection http = (HttpURLConnection) con;
        http.setRequestMethod("POST");
        http.setDoOutput(true);
        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        http.setFixedLengthStreamingMode(out.length);

        http.connect();

        try (OutputStream os = http.getOutputStream()) {
            os.write(out);
        }

        int statusCode = http.getResponseCode();
        if (statusCode >= 200 && statusCode < 400) {
            try (InputStream is = http.getInputStream()) {
                String response = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
                System.out.println(response);
            }
        } else {
            try (InputStream is = http.getErrorStream()) {
                String response = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
                System.out.println(response);
            }
        }
    }

    /**
     * Create a new Graql shell
     */
    GraqlShell(
            String historyFilename, String keyspace, GraqlClient client, URI uri, Optional<List<String>> queryStrings,
            String outputFormat
    ) throws Throwable {

        this.historyFilename = historyFilename;

        try {
            console = new ConsoleReader(System.in, System.out);

            // Create handler to handle SIGINT (Ctrl-C) interrupts
            Signal signal = new Signal("INT");
            GraqlSignalHandler signalHandler = new GraqlSignalHandler(this);
            Signal.handle(signal, signalHandler);

            try {
                session = client.connect(this, uri).get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }

            // Send the requested keyspace and output format to the server once connected
            sendJson(Json.object(
                    ACTION, ACTION_INIT,
                    KEYSPACE, keyspace,
                    OUTPUT_FORMAT, outputFormat
            ));

            // Start shell
            start(queryStrings);

        } finally {
            client.close();
            console.flush();
        }
    }

    private void start(Optional<List<String>> queryStrings) throws IOException {

        // Begin sending pings
        Thread thread = new Thread(this::ping);
        thread.setDaemon(true);
        thread.start();

        if (queryStrings.isPresent()) {
            queryStrings.get().forEach(queryString -> {
                executeQuery(queryString);
                commit();
            });
        } else {
            executeRepl();
        }
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
        File historyFile = new File(System.getProperty("java.io.tmpdir") + historyFilename);
        //noinspection ResultOfMethodCallIgnored
        historyFile.createNewFile();
        FileHistory history = new FileHistory(historyFile);
        console.setHistory(history);

        // Add all autocompleters
        console.addCompleter(new AggregateCompleter(graqlCompleter, new ShellCommandCompleter()));

        String queryString;

        java.util.regex.Pattern commandPattern = java.util.regex.Pattern.compile("\\s*(.*?)\\s*;?");

        while ((queryString = console.readLine()) != null) {
            history.flush();

            Matcher matcher = commandPattern.matcher(queryString);

            if (matcher.matches()) {
                switch (matcher.group(1)) {
                    case EDIT_COMMAND:
                        executeQuery(runEditor());
                        continue;
                    case COMMIT_COMMAND:
                        commit();
                        continue;
                    case ROLLBACK_COMMAND:
                        rollback();
                        continue;
                    case CLEAR_COMMAND:
                        console.clearScreen();
                        continue;
                    case LICENSE_COMMAND:
                        printLicense();
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
                String path = queryString.substring(LOAD_COMMAND.length() + 1);

                try {
                    queryString = loadQuery(path);
                } catch (IOException e) {
                    System.err.println(e.toString());
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

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) throws IOException, ExecutionException, InterruptedException {
        // 1000 = Normal close, 1001 = Going away
        if (statusCode != 1000 && statusCode != 1001) {
            System.err.println("Websocket closed, code: " + statusCode + ", reason: " + reason);
        }
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        Json json = Json.read(msg);

        switch (json.at(ACTION).asString()) {
            case ACTION_QUERY:
                String result = json.at(QUERY_RESULT).asString();
                print(result);
                break;
            case ACTION_END:
                // Alert the shell that the query has finished, so it can prompt for another query
                synchronized (this) {
                    notifyAll();
                }
                break;
            case ACTION_TYPES:
                Set<String> types = json.at(TYPES).asJsonList().stream().map(Json::asString).collect(toSet());
                graqlCompleter.setTypes(types);
                break;
            case ACTION_ERROR:
                System.err.print(json.at(ERROR).asString());
                break;
        }
    }

    private void ping() {
        try {
            // This runs on a daemon thread, so it will be terminated when the JVM stops
            //noinspection InfiniteLoopStatement
            while (true) {
                sendJson(Json.object(ACTION, ACTION_PING));

                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WebSocketException e) {
            // Report an error if the session is still open
            if (session.isOpen()) {
                throw new RuntimeException(e);
            }
        }
    }

    private void executeQuery(String queryString) {
        // Split query into chunks
        Iterable<String> splitQuery = Splitter.fixedLength(QUERY_CHUNK_SIZE).split(queryString);

        for (String queryChunk : splitQuery) {
            sendJson(Json.object(
                    ACTION, ACTION_QUERY,
                    QUERY, queryChunk
            ));
        }

        sendJson(Json.object(ACTION, ACTION_END));
        waitForEnd();
    }

    private void waitForEnd() {
        // Wait until the command is executed before continuing
        waitingQuery = true;
        synchronized (this) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        waitingQuery = false;
    }

    private void setDisplayOptions(Set<String> displayOptions) {
        sendJson(Json.object(
                ACTION, ACTION_DISPLAY,
                DISPLAY, displayOptions
        ));
    }

    private void commit() {
        sendJson(Json.object(ACTION, ACTION_COMMIT));
        waitForEnd();
    }

    private void rollback() {
        sendJson(Json.object(ACTION, ACTION_ROLLBACK));
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
            sendJson(Json.object(ACTION, ACTION_QUERY_ABORT));
            waitingQuery = false;
        } else {
            System.exit(0);
        }
    }

    private void sendJson(Json json) {
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
}
