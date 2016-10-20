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

package io.mindmaps.migration.base.io;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.engine.util.ConfigProperties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;
import java.util.Collections;

public class MigrationCLI {

    private static final ConfigProperties properties = ConfigProperties.getInstance();
    private static Options options = new Options();
    {
        options.addOption("h", "help", false, "print usage message");
        options.addOption("k", "keyspace", true, "keyspace to use");
        options.addOption("u", "uri", true, "uri to engine endpoint");
    }

    private CommandLine cmd;

    public MigrationCLI(String[] args){
        CommandLineParser parser = new DefaultParser();

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            die(e.getMessage());
        }

        if (cmd.hasOption("h") || cmd.getArgList().isEmpty()) {
            printHelpMessage();
        }
    }

    public String printInitMessage(String dataToMigrate){
        return "Migrating data " + dataToMigrate +
                " using MM Engine " + getEngineURI() +
                " into graph " + getKeyspace();
    }

    public String printCompletionMessage(){
        return "Migration Complete";
    }

    public String getEngineURI(){
        return cmd.getOptionValue("u", Mindmaps.DEFAULT_URI);
    }

    public String getKeyspace(){
        return cmd.getOptionValue("k", properties.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY));
    }

    public String getOption(String opt){
        return cmd.getOptionValue(opt);
    }

    public String getRequiredOption(String opt, String errorMessage){
        return hasOption(opt) ? getOption(opt) : die(errorMessage);
    }

    public boolean hasOption(String opt){
        return cmd.hasOption(opt);
    }

    public Loader getLoader(){
        return getEngineURI().equals(Mindmaps.DEFAULT_URI)
                ? new BlockingLoader(getKeyspace())
                : new DistributedLoader(getKeyspace(), Collections.singleton(getEngineURI()));
    }

    public MindmapsGraph getGraph(){
        return Mindmaps.factory(getEngineURI(), getKeyspace()).getGraph();
    }

    public static void addOption(String shortName, String longName, boolean arg, String desc){
        options.addOption(shortName,longName,arg,desc);
    }

    public static String die(String errorMsg) {
        printHelpMessage();
        throw new RuntimeException(errorMsg);
    }

    private static void printHelpMessage(){
        HelpFormatter helpFormatter = new HelpFormatter();
        PrintWriter printWriter = new PrintWriter(System.out);
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "migration.sh", null, options, leftPadding, descPadding, null);
        printWriter.flush();
    }
}
