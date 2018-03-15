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

package ai.grakn.graql.shell;

import ai.grakn.Grakn;
import ai.grakn.GraknConfigKey;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.Query;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.SimpleURI;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
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

/* uncover the secret

\u002a\u002f\u0069\u006d\u0070\u006f\u0072\u0074
\u0073\u0074\u0061\u0074\u0069\u0063
\u0061\u0069
\u002e
\u0067\u0072\u0061\u006b\u006e
\u002e
\u0067\u0072\u0061\u0071\u006c
\u002e
\u0069\u006e\u0074\u0065\u0072\u006e\u0061\u006c           /*        ,_,                   \u002a\u002f
\u002e                                                     /*       (0_0)_----------_      \u002a\u002f
\u0073\u0068\u0065\u006c\u006c                             /*      (_____)           |~'   \u002a\u002f
\u002e                                                     /*      `-'-'-'           /     \u002a\u002f
\u0061\u006e\u0069\u006d\u0061\u006c\u0069\u0061           /*        `|__|~-----~|__|      \u002a\u002f
\u002e
\u0063\u0068\u006f\u0072\u0064\u0061\u0074\u0061
\u002e
\u006d\u0061\u006d\u006d\u0061\u006c\u0069\u0061
\u002e
\u0061\u0072\u0074\u0069\u006f\u0064\u0061\u0063\u0074\u0079\u006c\u0061
\u002e
\u0068\u0069\u0070\u0070\u006f\u0070\u006f\u0074\u0061\u006d\u0069\u0064\u0061\u0065
\u002e
\u0048\u0069\u0070\u0070\u006f\u0070\u006f\u0074\u0061\u006d\u0075\u0073\u0046\u0061\u0063\u0074\u006f\u0072\u0079
\u002e \u0069\u006e\u0063\u0072\u0065\u0061\u0073\u0065\u0050\u006f\u0070 \u003b\u002f\u002a */

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

    /**
     * Array of available commands in shell
     */
    public static final ImmutableList<String> COMMANDS = ImmutableList.of(
            EDIT_COMMAND, COMMIT_COMMAND, ROLLBACK_COMMAND, LOAD_COMMAND, DISPLAY_COMMAND, CLEAR_COMMAND, EXIT_COMMAND,
            LICENSE_COMMAND, CLEAN_COMMAND
    );

    private static final String HISTORY_FILENAME = StandardSystemProperty.USER_HOME.value() + "/.graql-history";

    private final OutputFormat outputFormat;
    private final boolean infer;
    private ConsoleReader console;

    private final HistoryFile historyFile;

    private final GraknSession session;
    private GraknTx tx;
    private Set<AttributeType<?>> displayAttributes = ImmutableSet.of();

    private final GraqlCompleter graqlCompleter;
    private final ExternalEditor editor = ExternalEditor.create();

    private boolean errorOccurred = false;

    /**
     * Run a Graql REPL
     *
     * @param args arguments to the Graql shell. Possible arguments can be listed by running {@code graql console --help}
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        boolean success = runShell(args, GraknVersion.VERSION, HISTORY_FILENAME, GraknConfig.create());
        System.exit(success ? 0 : 1);
    }

    public static boolean runShell(
            String[] args, String version, String historyFilename, GraknConfig config
    ) throws InterruptedException, IOException {

        GraqlShellOptions options;

        try {
            options = GraqlShellOptions.create(args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return false;
        }

        List<String> queries = null;

        String query = options.getQuery();

        // This is a best-effort guess as to whether the user has made a mistake, without parsing the query
        if (query != null) {
            queries = ImmutableList.of(query);

            if (!query.contains("$") && query.trim().startsWith("match")) {
                System.err.println(ErrorMessage.NO_VARIABLE_IN_QUERY.getMessage());
            }
        }

        List<Path> filePaths = options.getFiles();

        // Print usage message if requested or if invalid arguments provided
        if (options.displayHelp()) {
            GraqlShellOptions.printUsage();
            return true;
        }

        if (options.displayVersion()) {
            System.out.println(version);
            return true;
        }

        Keyspace keyspace = options.getKeyspace();

        int defaultGrpcPort = config.getProperty(GraknConfigKey.GRPC_PORT);
        SimpleURI defaultGrpcUri = new SimpleURI(Grakn.DEFAULT_URI.getHost(), defaultGrpcPort);

        SimpleURI location = options.getUri();
        OutputFormat outputFormat = options.getOutputFormat();

        SimpleURI httpUri = location != null ? location : Grakn.DEFAULT_URI;
        SimpleURI grpcUri = location != null ? location : defaultGrpcUri;

        boolean infer = options.shouldInfer();

        Path path = options.getBatchLoadPath();

        if (path != null) {
            try {
                BatchLoader.sendBatchRequest(httpUri, keyspace, path);
            } catch (Exception e) {
                System.out.println("Batch failed \n" + CommonUtil.simplifyExceptionMessage(e));
                return false;
            }
            return true;
        }

        try (GraqlShell shell = new GraqlShell(historyFilename, keyspace, grpcUri, outputFormat, infer)) {
            if (filePaths != null) {
                queries = loadQueries(filePaths);
            }

            // Start shell
            shell.start(queries);
            return !shell.errorOccurred;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Status.Code.UNAVAILABLE)) {
                System.err.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
                return false;
            } else {
                throw e;
            }
        }
    }

    private static List<String> loadQueries(Iterable<Path> filePaths) throws IOException {
        List<String> queries = Lists.newArrayList();

        for (Path filePath : filePaths) {
            queries.add(loadQuery(filePath));
        }

        return queries;
    }

    private static String loadQuery(Path filePath) throws IOException {
        List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        return lines.stream().collect(joining("\n"));
    }

    /**
     * Create a new Graql shell
     */
    private GraqlShell(
            String historyFilename, Keyspace keyspace, SimpleURI uri, OutputFormat outputFormat, boolean infer
    ) throws IOException {
        this.outputFormat = outputFormat;
        this.infer = infer;
        console = new ConsoleReader(System.in, System.out);

        session = RemoteGrakn.session(uri, keyspace);
        tx = session.open(GraknTxType.WRITE);

        graqlCompleter = GraqlCompleter.create(session);
        historyFile = HistoryFile.create(console, historyFilename);
    }

    private void start(@Nullable List<String> queryStrings) throws IOException, InterruptedException {
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
                    /*
                    \u002a\u002f\u0063\u0061\u0073\u0065\u0020

                    "HIPPO"

                    \u003a\u0020\u0069\u006e\u0063\u0072\u0065\u0061\u0073\u0065\u0050\u006f\u0070\u0028
                    \u0063\u006f\u006e\u0073\u006f\u006c\u0065\u0029\u003b
                    \u0020\u0063\u006f\u006e\u0074\u0069\u006e\u0075\u0065\u003b\u002f\u002a
                    */
                }
            }

            // Load from a file if load command used
            if (queryString.startsWith(LOAD_COMMAND + " ")) {
                String pathString = queryString.substring(LOAD_COMMAND.length() + 1);
                Path path = Paths.get(unescapeJavaScript(pathString));

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

    private void executeQuery(String queryString) throws IOException {
        GraqlConverter<?, String> converter = outputFormat.getConverter(displayAttributes);

        handleGraknExceptions(() -> {
            Stream<Query<?>> queries = tx.graql().infer(infer).parser().parseList(queryString);

            Iterable<String> results = () -> queries.flatMap(query -> query.results(converter)).iterator();

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
            System.err.println(e.getMessage());
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

    @Override
    public final void close() throws IOException {
        tx.close();
        session.close();
        historyFile.close();
    }
}
