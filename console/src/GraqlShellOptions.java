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

package ai.grakn.core.console;

import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableList;

/**
 * Wrapper for parsing command-line options for Graql shell
 *
 * @author Felix Chapman
 */
public class GraqlShellOptions {

    public static final Keyspace DEFAULT_KEYSPACE = Keyspace.of("grakn");

    private static final String KEYSPACE = "k";
    private static final String EXECUTE = "e";
    private static final String FILE = "f";
    private static final String URI = "r";
    private static final String OUTPUT = "o";
    private static final String NO_INFER = "n";
    private static final String HELP = "h";
    private static final String VERSION = "v";

    private final Options options;
    private final CommandLine cmd;

    private GraqlShellOptions(Options options, CommandLine cmd) {
        this.options = options;
        this.cmd = cmd;
    }

    private static Options defaultOptions() {
        Options options = new Options();
        options.addOption(KEYSPACE, "keyspace", true, "keyspace of the graph");
        options.addOption(EXECUTE, "execute", true, "query to execute");
        options.addOption(FILE, "file", true, "graql file path to execute");
        options.addOption(URI, "uri", true, "uri to factory to engine");
        options.addOption(OUTPUT, "output", true, "output format for results");
        options.addOption(NO_INFER, "no_infer", false, "do not perform inference on results");
        options.addOption(HELP, "help", false, "print usage message");
        options.addOption(VERSION, "version", false, "print version");
        return options;
    }

    public static GraqlShellOptions create(String[] args) throws ParseException {
        return create(args, new Options());
    }

    public static GraqlShellOptions create(String[] args, Options additionalOptions) throws ParseException {
        Options options = defaultOptions();
        for (Option option : additionalOptions.getOptions()) {
            options.addOption(option);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return new GraqlShellOptions(options, cmd);
    }

    public CommandLine cmd() {
        return cmd;
    }

    public void printUsage(PrintStream sout) {
        HelpFormatter helpFormatter = new HelpFormatter();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(sout, Charset.defaultCharset());
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(outputStreamWriter));
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "grakn console", null, options, leftPadding, descPadding, null);
        printWriter.flush();
    }

    public boolean shouldInfer() {
        return !cmd.hasOption(NO_INFER);
    }

    public OutputFormat getOutputFormat() {
        String format = cmd.getOptionValue(OUTPUT);
        return format != null ? OutputFormat.get(format) : OutputFormat.DEFAULT;
    }

    @Nullable
    public SimpleURI getUri() {
        String uri = cmd.getOptionValue(URI);
        return uri != null ? new SimpleURI(uri) : null;
    }

    public boolean displayVersion() {
        return cmd.hasOption(VERSION);
    }

    public boolean displayHelp() {
        return cmd.hasOption(HELP) || !cmd.getArgList().isEmpty();
    }

    public Keyspace getKeyspace() {
        String keyspace = cmd.getOptionValue(KEYSPACE);
        return keyspace != null ? Keyspace.of(keyspace) : DEFAULT_KEYSPACE;
    }

    @Nullable
    public List<Path> getFiles() {
        String[] paths = cmd.getOptionValues(FILE);
        return paths != null ? Stream.of(paths).map(Paths::get).collect(toImmutableList()) : null;
    }

    @Nullable
    public String getQuery() {
        return cmd.getOptionValue(EXECUTE);
    }
}
