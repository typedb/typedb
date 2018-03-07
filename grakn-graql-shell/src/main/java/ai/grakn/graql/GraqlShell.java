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

package ai.grakn.graql;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.BatchExecutorClient;
import ai.grakn.client.Client;
import ai.grakn.client.GraknClient;
import ai.grakn.client.QueryResponse;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.graql.internal.shell.ErrorMessage;
import ai.grakn.graql.internal.shell.GraqlCompleter;
import ai.grakn.graql.internal.shell.ShellCommandCompleter;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.history.FileHistory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.Charsets;
import rx.Observable;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.shell.animalia.chordata.mammalia.artiodactyla.hippopotamidae.HippopotamusFactory.increasePop;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.Schema.BaseType.TYPE;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang.StringEscapeUtils.unescapeJavaScript;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * A Graql REPL shell that can be run from the command line
 *
 * @author Felix Chapman
 */
public class GraqlShell implements AutoCloseable {
    private static final String LICENSE_PROMPT = "\n" +
            "Grakn  Copyright (C) 2018  Grakn Labs Limited \n" +
            "This is free software, and you are welcome to redistribute it \n" +
            "under certain conditions; type 'license' for details.\n";

    private static final String LICENSE_LOCATION = "LICENSE.txt";

    public static final String DEFAULT_KEYSPACE = "grakn";
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
    private static final String CLEAN_COMMAND = "clean";
    private static final String HI_POP_COMMAND =
            HAS.name().substring(0, 1) + Integer.class.getSimpleName().substring(0, 1) +
                    Strings.repeat(TYPE.name().substring(2, 3), 2) + Object.class.getSimpleName().substring(0, 1);

    /**
     * Array of available commands in shell
     */
    public static final ImmutableList<String> COMMANDS = ImmutableList.of(
            EDIT_COMMAND, COMMIT_COMMAND, ROLLBACK_COMMAND, LOAD_COMMAND, DISPLAY_COMMAND, CLEAR_COMMAND, EXIT_COMMAND,
            LICENSE_COMMAND, CLEAN_COMMAND
    );

    private static final String TEMP_FILENAME = "/graql-tmp.gql";
    private static final String HISTORY_FILENAME = StandardSystemProperty.USER_HOME.value() + "/.graql-history";

    private static final String DEFAULT_EDITOR = "vim";
    private static final int DEFAULT_MAX_RETRY = 1;

    private final File tempFile = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value() + TEMP_FILENAME);
    private final String outputFormat;
    private final boolean infer;
    private ConsoleReader console;

    private final String historyFilename;

    private final GraknSession session;
    private GraknTx tx;
    private Set<AttributeType<?>> displayAttributes = ImmutableSet.of();

    private final GraqlCompleter graqlCompleter = new GraqlCompleter();

    private boolean errorOccurred = false;

    /**
     * Run a Graql REPL
     *
     * @param args arguments to the Graql shell. Possible arguments can be listed by running {@code graql console --help}
     */
    public static void main(String[] args) {
        boolean success = runShell(args, GraknVersion.VERSION, HISTORY_FILENAME, GraknConfig.create());
        System.exit(success ? 0 : 1);
    }

    public static boolean runShell(String[] args, String version, String historyFilename, GraknConfig config) {

        Options options = new Options();
        options.addOption("k", "keyspace", true, "keyspace of the graph");
        options.addOption("e", "execute", true, "query to execute");
        options.addOption("f", "file", true, "graql file path to execute");
        options.addOption("r", "uri", true, "uri to factory to engine");
        options.addOption("b", "batch", true, "graql file path to batch load");
        options.addOption("s", "size", true, "the size of the batches (must be used with -b)");
        options.addOption("a", "active", true, "the number of active tasks (must be used with -b)");
        options.addOption("o", "output", true, "output format for results");
        options.addOption("n", "no_infer", false, "do not perform inference on results");
        options.addOption("h", "help", false, "print usage message");
        options.addOption("v", "version", false, "print version");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return false;
        }

        Optional<List<String>> queries = Optional.ofNullable(cmd.getOptionValue("e")).map(Lists::newArrayList);

        if (queries.isPresent()) {
            for (String query : queries.get()) {
                // This is a best-effort guess as to whether the user has made a mistake, without parsing the query
                if (!query.contains("$") && query.trim().startsWith("match")) {
                    System.err.println(ErrorMessage.NO_VARIABLE_IN_QUERY.getMessage());
                    break;
                }
            }
        }

        String[] filePaths = cmd.getOptionValues("f");

        // Print usage message if requested or if invalid arguments provided
        if (cmd.hasOption("h") || !cmd.getArgList().isEmpty()) {
            printUsage(options, null);
            return true;
        }

        if (cmd.hasOption("v")) {
            System.out.println(version);
            return true;
        }

        Keyspace keyspace = Keyspace.of(cmd.getOptionValue("k", DEFAULT_KEYSPACE));

        int defaultGrpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        SimpleURI defaultGrpcUri = new SimpleURI(Grakn.DEFAULT_URI.getHost(), defaultGrpcPort);

        Optional<SimpleURI> location = Optional.ofNullable(cmd.getOptionValue("r")).map(SimpleURI::new);
        String outputFormat = cmd.getOptionValue("o", DEFAULT_OUTPUT_FORMAT);

        SimpleURI httpUri = location.orElse(Grakn.DEFAULT_URI);
        SimpleURI grpcUri = location.orElse(defaultGrpcUri);

        if (!Client.serverIsRunning(httpUri)) {
            System.err.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
            return false;
        }

        boolean infer = !cmd.hasOption("n");

        if (cmd.hasOption("b")) {
            try {
                sendBatchRequest(loaderClient(httpUri), cmd.getOptionValue("b"), keyspace);
            } catch (NumberFormatException e) {
                printUsage(options, "Cannot cast argument to an integer " + e.getMessage());
                return false;
            } catch (Exception e) {
                System.out.println("Batch failed \n" + CommonUtil
                        .simplifyExceptionMessage(e));
                return false;
            }
            return true;
        } else if (cmd.hasOption("a") || cmd.hasOption("s")) {
            printUsage(options, "The active or size option has been specified without batch.");
            return false;
        }


        try (GraqlShell shell = new GraqlShell(historyFilename, keyspace, grpcUri, outputFormat, infer)) {
            if (filePaths != null) {
                queries = Optional.of(loadQueries(filePaths));
            }

            // Start shell
            shell.start(queries);
            return !shell.errorOccurred;
        } catch (java.net.ConnectException e) {
            System.err.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
            return false;
        } catch (Throwable e) {
            System.err.println(getFullStackTrace(e));
            return false;
        }
    }

    private static void printUsage(Options options, @Nullable String footer) {
        HelpFormatter helpFormatter = new HelpFormatter();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(System.out, Charset.defaultCharset());
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(outputStreamWriter));
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "graql console", null, options, leftPadding, descPadding, footer);
        printWriter.flush();
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
        return lines.stream().collect(joining("\n"));
    }

    private static void sendBatchRequest(BatchExecutorClient batchExecutorClient, String graqlPath, Keyspace keyspace) throws IOException {
        AtomicInteger queriesExecuted = new AtomicInteger(0);

        FileInputStream inputStream = new FileInputStream(Paths.get(graqlPath).toFile());

        try (Reader queryReader = new InputStreamReader(inputStream, Charsets.UTF_8)) {
            Graql.parser().parseList(queryReader).forEach(query -> {
                Observable<QueryResponse> observable = batchExecutorClient.add(query, keyspace, false);

                observable.subscribe(
                    /* On success: */ queryResponse -> queriesExecuted.incrementAndGet(),
                    /* On error:   */ System.err::println
                );
            });
        }

        batchExecutorClient.close();

        System.out.println("Statements executed: " + queriesExecuted.get());
    }

    /**
     * Create a new Graql shell
     */
    GraqlShell(
            String historyFilename, Keyspace keyspace, SimpleURI uri, String outputFormat, boolean infer
    ) throws Throwable {

        this.historyFilename = historyFilename;
        this.outputFormat = outputFormat;
        this.infer = infer;
        console = new ConsoleReader(System.in, System.out);

        session = RemoteGrakn.session(uri, keyspace);
        tx = session.open(GraknTxType.WRITE);

        Set<String> types = getTypes(tx).map(Label::getValue).collect(toImmutableSet());
        graqlCompleter.setTypes(types);
    }

    public static BatchExecutorClient loaderClient(SimpleURI uri) {
        return BatchExecutorClient.newBuilder()
                .threadPoolCoreSize(Runtime.getRuntime().availableProcessors() * 8)
                .taskClient(GraknClient.of(uri))
                .maxRetries(DEFAULT_MAX_RETRY)
                .build();
    }

    private GraqlConverter<?, String> printer() {
        switch (outputFormat) {
            case "json":
                return Printers.json();
            case "graql":
            default:
                AttributeType<?>[] array = displayAttributes.toArray(new AttributeType[displayAttributes.size()]);
                return Printers.graql(true, array);
        }
    }

    private void start(Optional<List<String>> queryStrings) throws IOException {
        try {
            if (queryStrings.isPresent()) {
                for (String queryString : queryStrings.get()) {
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
    void executeRepl() throws IOException {
        console.print(LICENSE_PROMPT);

        // Disable JLine feature when seeing a '!', which is used in our queries
        console.setExpandEvents(false);

        console.setPrompt(PROMPT);

        // Create temporary file
        if (!tempFile.exists()) {
            boolean success = tempFile.createNewFile();
            if (!success) println(ErrorMessage.COULD_NOT_CREATE_TEMP_FILE.getMessage());
        }

        setupHistory();

        // Add all autocompleters
        console.addCompleter(new AggregateCompleter(graqlCompleter, new ShellCommandCompleter()));

        String queryString;

        java.util.regex.Pattern commandPattern = java.util.regex.Pattern.compile("\\s*(.*?)\\s*;?");

        while ((queryString = console.readLine()) != null) {
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
                    case CLEAN_COMMAND:
                        clean();
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

            if (queryString.equals(HI_POP_COMMAND)) {
                increasePop(console);
                continue;
            }

            // Load from a file if load command used
            if (queryString.startsWith(LOAD_COMMAND + " ")) {
                String path = queryString.substring(LOAD_COMMAND.length() + 1);
                path = unescapeJavaScript(path);

                try {
                    queryString = loadQuery(path);
                } catch (IOException e) {
                    System.err.println(e.toString());
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

    private boolean setupHistory() throws IOException {
        // Create history file
        File historyFile = new File(historyFilename);
        boolean fileCreated = historyFile.createNewFile();
        FileHistory history = new FileHistory(historyFile);
        console.setHistory(history);

        // Make sure history is saved on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                history.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        return fileCreated;
    }

    private void printLicense() {
        StringBuilder result = new StringBuilder("");

        //Get file from resources folder
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(LICENSE_LOCATION);

        Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name());
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.append(line).append("\n");
        }
        result.append("\n");
        scanner.close();

        this.println(result.toString());
    }

    private void executeQuery(String queryString) throws IOException {
        handleGraknExceptions(() -> {
            Stream<Query<?>> queries = tx.graql().infer(infer).parser().parseList(queryString);
            queries.flatMap(query -> query.results(printer())).forEach(this::println);
        });

        // Flush the console so the output is all displayed before the next command
        console.flush();
    }

    private void setDisplayOptions(Set<String> displayOptions) throws IOException {
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

    /**
     * load the user's preferred editor to edit a query
     *
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

    private void handleGraknExceptions(Runnable runnable) {
        try {
            runnable.run();
        } catch (GraknException e) {
            System.err.println(e);
            errorOccurred = true;
            reopenTx();
        }
    }

    private void println(String string) {
        try {
            console.println(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<Label> getTypes(GraknTx tx) {
        return tx.admin().getMetaConcept().subs().map(SchemaConcept::getLabel);
    }

    private void reopenTx() {
        if (!tx.isClosed()) tx.close();
        tx = session.open(GraknTxType.WRITE);
    }

    @Override
    public final void close() throws Exception {
        tx.close();
        session.close();
    }
}
