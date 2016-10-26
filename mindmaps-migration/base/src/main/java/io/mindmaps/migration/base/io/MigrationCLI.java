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
import io.mindmaps.concept.Concept;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.QueryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;
import java.util.Collections;

import static io.mindmaps.graql.Graql.count;
import static io.mindmaps.graql.Graql.var;

public class MigrationCLI {

    private static final ConfigProperties properties = ConfigProperties.getInstance();
    private Options defaultOptions = new Options();
    {
        defaultOptions.addOption("h", "help", false, "print usage message");
        defaultOptions.addOption("k", "keyspace", true, "keyspace to use");
        defaultOptions.addOption("u", "uri", true, "uri to engine endpoint");
    }

    private CommandLine cmd;

    public MigrationCLI(String[] args, Options options){
        addOptions(options);
        CommandLineParser parser = new DefaultParser();

        try {
            cmd = parser.parse(defaultOptions, args);
        } catch (ParseException e) {
            die(e.getMessage());
        }

        if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
            printHelpMessage();
        }
    }

    public void printInitMessage(String dataToMigrate){
        System.out.println("Migrating data " + dataToMigrate +
                " using MM Engine " + getEngineURI() +
                " into graph " + getKeyspace());
    }

    public void printCompletionMessage(){
        MindmapsGraph graph = getGraph();
        QueryBuilder qb = Graql.withGraph(graph);

        StringBuilder builder = new StringBuilder();
        builder.append("Graph ontology contains:\n");
        builder.append("\t ").append(graph.getMetaEntityType().instances().size()).append(" entity types\n");
        builder.append("\t ").append(graph.getMetaRelationType().instances().size()).append(" relation types\n");
        builder.append("\t ").append(graph.getMetaRoleType().instances().size()).append(" role types\n");
        builder.append("\t ").append(graph.getMetaResourceType().instances().size()).append(" resource types\n");
        builder.append("\t ").append(graph.getMetaRuleType().instances().size()).append(" rule types\n\n");

        builder.append("Graph data contains:\n");
        builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("entity-type")).aggregate(count()).execute()).append(" entities\n");
        builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("relation-type")).aggregate(count()).execute()).append(" relations\n");
        builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("resource-type")).aggregate(count()).execute()).append(" resources\n");
        builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("rule-type")).aggregate(count()).execute()).append(" rules\n\n");

        builder.append("Migration complete");
        System.out.println(builder);

        graph.close();
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

    public void addOptions(Options options){
        options.getOptions().forEach(defaultOptions::addOption);
    }

    public String die(String errorMsg) {
        printHelpMessage();
        throw new RuntimeException(errorMsg);
    }

    private void printHelpMessage(){
        HelpFormatter helpFormatter = new HelpFormatter();
        PrintWriter printWriter = new PrintWriter(System.out);
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "migration.sh", null, defaultOptions, leftPadding, descPadding, null);
        printWriter.flush();
    }
}
