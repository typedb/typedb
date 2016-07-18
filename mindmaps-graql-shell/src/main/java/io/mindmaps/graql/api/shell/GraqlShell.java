package io.mindmaps.graql.api.shell;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.InvalidConceptTypeException;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.factory.MindmapsGraphFactory;
import io.mindmaps.factory.MindmapsTitanGraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.AskQuery;
import io.mindmaps.graql.api.query.DeleteQuery;
import io.mindmaps.graql.api.query.InsertQuery;
import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.internal.parser.ANSI;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import io.mindmaps.graql.internal.shell.*;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.history.FileHistory;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Graql REPL shell that can be run from the command line
 */
public class GraqlShell implements AutoCloseable {

    private static final String GRAPH_CONF = "/opt/mindmaps/resources/conf/titan-cassandra-es.properties";

    private static final String PROMPT = ">>> ";

    private static final String EDIT_COMMAND = "edit";
    private static final String COMMIT_COMMAND = "commit";
    private static final String LOAD_COMMAND = "load";
    private static final String CLEAR_COMMAND = "clear";
    private static final String EXIT_COMMAND = "exit";

    /**
     * Array of available commands in shell
     */
    public static final String[] COMMANDS = {EDIT_COMMAND, COMMIT_COMMAND, LOAD_COMMAND, CLEAR_COMMAND, EXIT_COMMAND};

    private static final String TEMP_FILENAME = "/graql-tmp.gql";
    private static final String HISTORY_FILENAME = "/graql-history";

    private static final String DEFAULT_EDITOR = "vim";

    private final File tempFile = new File(System.getProperty("java.io.tmpdir") + TEMP_FILENAME);
    private ConsoleReader console;
    private PrintStream err;

    private final MindmapsGraph graph;
    private final MindmapsTransaction transaction;

    /**
     * Run a Graql REPL
     * @param args arguments to the Graql shell. Possible arguments can be listed by running {@code graql.sh --help}
     */
    public static void main(String[] args) {
        String version = GraqlShell.class.getPackage().getImplementationVersion();
        MindmapsGraphFactory graphFactory = MindmapsTitanGraphFactory.getInstance();
        runShell(args, graphFactory, version, System.in, System.out, System.err);
    }

    static void runShell(
            String[] args, MindmapsGraphFactory factory, String version,
            InputStream in, PrintStream out, PrintStream err
    ) {
        // Disable horrid cassandra logs
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.OFF);

        Options options = new Options();
        options.addOption("c", "config", true, "path to a graph config file");
        options.addOption("e", "execute", true, "query to execute");
        options.addOption("f", "file", true, "graql file path to execute");
        options.addOption("h", "help", false, "print usage message");
        options.addOption("v", "version", false, "print version");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            err.println(e.getMessage());
            return;
        }

        String graphConf = cmd.getOptionValue("c", GRAPH_CONF);
        String query = cmd.getOptionValue("e");
        String filePath = cmd.getOptionValue("f");

        // Print usage message if requested or if invalid arguments provided
        if (cmd.hasOption("h") || !cmd.getArgList().isEmpty()) {
            HelpFormatter helpFormatter = new HelpFormatter();
            PrintWriter printWriter = new PrintWriter(out);
            int width = helpFormatter.getWidth();
            int leftPadding = helpFormatter.getLeftPadding();
            int descPadding = helpFormatter.getDescPadding();
            helpFormatter.printHelp(printWriter, width, "graql.sh", null, options, leftPadding, descPadding, null);
            printWriter.flush();
            return;
        }

        if (cmd.hasOption("v")) {
            out.println(version);
            return;
        }

        MindmapsGraph graph = factory.newGraph(graphConf);

        try(GraqlShell shell = new GraqlShell(graph, in, out, err)) {
            if (filePath != null) {
                query = loadQuery(filePath);
            }

            if (query != null) {
                shell.executeQuery(query, false);
                shell.commit();
            } else {
                shell.executeRepl();
            }
        } catch (IOException e) {
            err.println(e.toString());
        }
    }

    private static String loadQuery(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
        return lines.stream().collect(Collectors.joining("\n"));
    }

    /**
     * Create a new Graql shell
     * @param graph the graph to operate on
     */
    GraqlShell(MindmapsGraph graph, InputStream in, OutputStream out, PrintStream err) throws IOException {
        this.graph = graph;
        transaction = graph.newTransaction();
        console = new ConsoleReader(in, out);
        this.err = err;
    }

    @Override
    public void close() throws IOException {
        graph.close();
        console.flush();
    }

    /**
     * Run a Read-Evaluate-Print loop until the input terminates
     */
    void executeRepl() throws IOException {
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
        console.addCompleter(new AggregateCompleter(
                new KeywordCompleter(), new VariableCompleter(), new TypeCompleter(QueryBuilder.build(transaction)),
                new ShellCommandCompleter()
        ));

        String queryString;

        while ((queryString = console.readLine()) != null) {
            history.flush();

            switch (queryString) {
                case EDIT_COMMAND:
                    executeQuery(runEditor(), true);
                    break;
                case COMMIT_COMMAND:
                    commit();
                    break;
                case CLEAR_COMMAND:
                    console.clearScreen();
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
                            err.println(e.toString());
                            break;
                        }
                    }

                    executeQuery(queryString, true);
                    break;
            }
        }
    }

    private void executeQuery(String queryString, boolean setLimit) {
        Object query;

        try {
            QueryParser parser = QueryParser.create(transaction);
            query = parser.parseQuery(queryString);

            if (query instanceof MatchQueryPrinter) {
                printMatchQuery((MatchQueryPrinter) query, setLimit);
            } else if (query instanceof AskQuery) {
                printAskQuery((AskQuery) query);
            } else if (query instanceof InsertQuery) {
                printInsertQuery((InsertQuery) query);
            } else if (query instanceof DeleteQuery) {
                ((DeleteQuery) query).execute();
            } else {
                throw new RuntimeException("Unrecognized query " + query);
            }
        } catch (IllegalArgumentException | IllegalStateException | InvalidConceptTypeException e) {
            err.println(e.getMessage());
        }
    }

    private void printMatchQuery(MatchQueryPrinter matchQuery, boolean setLimit) {
        Stream<String> results = matchQuery.resultsString();
        if (setLimit) results = results.limit(100);
        results.forEach(this::println);
    }

    private void printAskQuery(AskQuery askQuery) {
        if (askQuery.execute()) {
            println(ANSI.color("True", ANSI.GREEN));
        } else {
            println(ANSI.color("False", ANSI.RED));
        }
    }

    private void printInsertQuery(InsertQuery insertQuery) {
        insertQuery.stream().map(Concept::getId).forEach(this::println);
    }

    private void commit() {
        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            err.println(e.getMessage());
        }
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
