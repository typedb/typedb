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
import grakn.core.client.Grakn;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.GraknVersion;
import io.grpc.Status;
import org.apache.commons.cli.CommandLine;
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
 * Grakn Console is a Command Line Application to interact with the Grakn Core database
 */
public class GraknConsole {

    public static final String DEFAULT_KEYSPACE = "grakn";

    private static final String KEYSPACE = "k";
    private static final String FILE = "f";
    private static final String URI = "r";
    private static final String NO_INFER = "n";
    private static final String HELP = "h";
    private static final String VERSION = "v";

    private final Options options = getOptions();
    private final CommandLine commandLine;
    private final PrintStream printOut;
    private final PrintStream printErr;

    private final Boolean infer;
    private final String serverAddress;
    private final String keyspace;

    public GraknConsole(String[] args, PrintStream printOut, PrintStream printErr) throws ParseException {
        this.printOut = printOut;
        this.printErr = printErr;

        this.commandLine = new DefaultParser().parse(options, args);
        this.infer = !commandLine.hasOption(NO_INFER);

        String serverAddressArg = commandLine.getOptionValue(URI);
        this.serverAddress = serverAddressArg != null ? serverAddressArg : Grakn.DEFAULT_URI;

        String keyspaceArg = commandLine.getOptionValue(KEYSPACE);
        this.keyspace = keyspaceArg != null ? keyspaceArg : DEFAULT_KEYSPACE;
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption(KEYSPACE, "keyspace", true, "keyspace of the graph");
        options.addOption(FILE, "file", true, "graql file path to execute");
        options.addOption(URI, "address", true, "Grakn Server address");
        options.addOption(NO_INFER, "no_infer", false, "do not perform inference on results");
        options.addOption(HELP, "help", false, "print usage message");
        options.addOption(VERSION, "version", false, "print version");

        return options;
    }

    public void run() throws InterruptedException, IOException {
        // Print usage guidelines for Grakn Console
        if (commandLine.hasOption(HELP) || !commandLine.getArgList().isEmpty()) {
            printHelp(printOut);
        }
        // Print Grakn Console version
        else if (commandLine.hasOption(VERSION)) {
            printOut.println(GraknVersion.VERSION);
        }
        // Start a Console Session to load some Graql file(s)
        else if (commandLine.hasOption(FILE)) {
            try (ConsoleSession consoleSession = new ConsoleSession(serverAddress, keyspace, infer, printOut, printErr)) {
                String[] paths = commandLine.getOptionValues(FILE);
                List<Path> filePaths = Stream.of(paths).map(Paths::get).collect(toImmutableList());
                for (Path file : filePaths) consoleSession.load(file);
            }
        }
        // Start a live Console Session for the user to interact with Grakn
        else {
            try (ConsoleSession consoleSession = new ConsoleSession(serverAddress, keyspace, infer, printOut, printErr)) {
                consoleSession.run();
            }
        }
    }

    private void printHelp(PrintStream sout) {
        HelpFormatter formatter = new HelpFormatter();
        OutputStreamWriter writer = new OutputStreamWriter(sout, Charset.defaultCharset());
        PrintWriter printer = new PrintWriter(new BufferedWriter(writer));
        formatter.printHelp(
                printer, formatter.getWidth(), "grakn console [options]", null,
                options, formatter.getLeftPadding(), formatter.getDescPadding(), null
        );
        printer.flush();
    }

    /**
     * Invocation from bash script './grakn console'
     */
    public static void main(String[] args) {
        // Disable logging for Grakn console as we only use System.out
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.OFF);

        try {
            GraknConsole console = new GraknConsole(Arrays.copyOfRange(args, 1, args.length), System.out, System.err);
            console.run();

            System.exit(0);
        } catch (Exception e) {
            if (e.getMessage().startsWith(Status.Code.UNAVAILABLE.name())) {
                System.err.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
            } else {
                System.err.println(e.getMessage());
            }

            System.exit(1);
        }
    }
}
