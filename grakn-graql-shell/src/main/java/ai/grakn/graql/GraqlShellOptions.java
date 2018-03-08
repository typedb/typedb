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

import ai.grakn.Keyspace;
import ai.grakn.util.SimpleURI;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
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

    private CommandLine cmd;

    private GraqlShellOptions(CommandLine cmd) {
        this.cmd = cmd;
    }

    private static Options options() {
        Options options = new Options();
        options.addOption("k", "keyspace", true, "keyspace of the graph");
        options.addOption("e", "execute", true, "query to execute");
        options.addOption("f", "file", true, "graql file path to execute");
        options.addOption("r", "uri", true, "uri to factory to engine");
        options.addOption("b", "batch", true, "graql file path to batch load");
        options.addOption("o", "output", true, "output format for results");
        options.addOption("n", "no_infer", false, "do not perform inference on results");
        options.addOption("h", "help", false, "print usage message");
        options.addOption("v", "version", false, "print version");
        return options;
    }

    public static GraqlShellOptions create(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options(), args);
        return new GraqlShellOptions(cmd);
    }

    public static void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(System.out, Charset.defaultCharset());
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(outputStreamWriter));
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "graql console", null, options(), leftPadding, descPadding, null);
        printWriter.flush();
    }

    @Nullable
    public Path getBatchLoadPath() {
        String path = cmd.getOptionValue("b");
        return path != null ? Paths.get(path) : null;
    }

    public boolean shouldInfer() {
        return !cmd.hasOption("n");
    }

    public OutputFormat getOutputFormat() {
        String format = cmd.getOptionValue("o");
        return format != null ? OutputFormat.get(format) : OutputFormat.DEFAULT;
    }

    @Nullable
    public SimpleURI getUri() {
        String uri = cmd.getOptionValue("r");
        return uri != null ? new SimpleURI(uri) : null;
    }

    public boolean displayVersion() {
        return cmd.hasOption("v");
    }

    public boolean displayHelp() {
        return cmd.hasOption("h") || !cmd.getArgList().isEmpty();
    }

    public Keyspace getKeyspace() {
        String keyspace = cmd.getOptionValue("k");
        return keyspace != null ? Keyspace.of(keyspace) : DEFAULT_KEYSPACE;
    }

    @Nullable
    public List<Path> getFiles() {
        String[] paths = cmd.getOptionValues("f");
        return paths != null ? Stream.of(paths).map(Paths::get).collect(toImmutableList()) : null;
    }

    @Nullable
    public String getQuery() {
        return cmd.getOptionValue("e");
    }
}
