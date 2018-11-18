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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Lists;
import grakn.core.client.Grakn;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.GraknVersion;
import io.grpc.Status;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;

/**
 * Grakn Console is a Command Line Interface to the Grakn Core database
 */
public class GraknConsole {

    public static final String DEFAULT_KEYSPACE = "grakn";

    private static final String KEYSPACE = "k";
    private static final String FILE = "f";
    private static final String URI = "r";
    private static final String NO_INFER = "n";
    private static final String HELP = "h";
    private static final String VERSION = "v";

    private final CommandLine commandLine;
    private final Options options;

    private GraknConsole(CommandLine commandLine, Options options) {
        this.commandLine = commandLine;
        this.options = options;
    }

    public static GraknConsole create(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(KEYSPACE, "keyspace", true, "keyspace of the graph");
        options.addOption(FILE, "file", true, "graql file path to execute");
        options.addOption(URI, "address", true, "Grakn Server address");
        options.addOption(NO_INFER, "no_infer", false, "do not perform inference on results");
        options.addOption(HELP, "help", false, "print usage message");
        options.addOption(VERSION, "version", false, "print version");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        return new GraknConsole(commandLine, options);
    }

    public boolean start(PrintStream printOut, PrintStream printErr) throws InterruptedException, IOException {

        // Print usage guidelines for Grakn Console
        if (commandLine.hasOption(HELP) || !commandLine.getArgList().isEmpty()) {
            printUsage(printOut);
            return true;
        }
        // Print Grakn Console version
        else if (commandLine.hasOption(VERSION)) {
            printOut.println(GraknVersion.VERSION);
            return true;
        }

        // Get the Grakn Server address (host + port)
        String serverAddress = commandLine.getOptionValue(URI);
        serverAddress = serverAddress != null ? serverAddress : Grakn.DEFAULT_URI;

        // Get the keyspace to access
        String keyspace = commandLine.getOptionValue(KEYSPACE);
        keyspace = keyspace != null ? keyspace : DEFAULT_KEYSPACE;

        String[] paths = commandLine.getOptionValues(FILE);
        List<Path> filePaths = paths != null ? Stream.of(paths).map(Paths::get).collect(toImmutableList()) : null;

        List<String> queries = null;
        if (filePaths != null) {
            queries = Lists.newArrayList();

            for (Path filePath : filePaths) {
                queries.add(ConsoleSession.loadQuery(filePath));
            }
        }

        try (ConsoleSession consoleSession = new ConsoleSession(serverAddress, keyspace, !commandLine.hasOption(NO_INFER), printOut, printErr)) {
            consoleSession.start(queries);
            return !consoleSession.errorOccurred();
        } catch (RuntimeException e) {
            if (e.getMessage().startsWith(Status.Code.UNAVAILABLE.name())) {
                printErr.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
            } else {
                printErr.println(e.getMessage());
            }
            return false;
        }
    }

    private void printUsage(PrintStream sout) {
        HelpFormatter helpFormatter = new HelpFormatter();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(sout, Charset.defaultCharset());
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(outputStreamWriter));
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "grakn console", null, options, leftPadding, descPadding, null);
        printWriter.flush();
    }

    /**
     * Invocation from bash script './grakn console'
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // Disable logging for Grakn console as we only use System.out
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.OFF);

        try {
            GraknConsole console = GraknConsole.create(Arrays.copyOfRange(args, 1, args.length));
            console.start(System.out, System.err);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
    }
}
