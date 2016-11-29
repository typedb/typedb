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

package ai.grakn.migration.base.io;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.loader.LoaderImpl;
import ai.grakn.engine.loader.client.LoaderClient;
import com.google.common.io.Files;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

public class MigrationCLI {

    private static final String COULD_NOT_CONNECT  = "Could not connect to Grakn Engine. Have you run 'grakn.sh start'?";

    private static final ConfigProperties properties = ConfigProperties.getInstance();
    private Options defaultOptions = new Options();
    {
        defaultOptions.addOption("v", "verbose", false, "Print counts of migrated data.");
        defaultOptions.addOption("h", "help", false, "Print usage message.");
        defaultOptions.addOption("k", "keyspace", true, "Grakn graph.");
        defaultOptions.addOption("u", "uri", true, "Location of Grakn Engine.");
        defaultOptions.addOption("n", "no", false, "Write to standard out.");
    }

    private CommandLine cmd;

    private MigrationCLI(String[] args, Options options){
        addOptions(options);
        CommandLineParser parser = new DefaultParser();

        try {
            cmd = parser.parse(defaultOptions, args);
        } catch (ParseException e) {
            die(e.getMessage());
        }

        if (cmd.hasOption("h")) {
            printHelpMessage();
        }

        if(cmd.getOptions().length == 0){
            printHelpMessage();
            throw new IllegalArgumentException("Helping");
        } else if(cmd.getOptions().length == 1 && cmd.hasOption("h")){
            throw new IllegalArgumentException("Helping");
        }

        if(!GraknEngineServer.isRunning()){
            System.out.println(COULD_NOT_CONNECT);
        }
    }

    public static Optional<MigrationCLI> create(String[] args, Options options){
        try {
            return Optional.of(new MigrationCLI(args, options));
        } catch (IllegalArgumentException e){
            return Optional.empty();
        }
    }

    public void writeToSout(Stream<InsertQuery> queries){
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));

        queries.map(InsertQuery::toString).forEach((str) -> {
            try {
                writer.write(str);
                writer.write("\n");
            }
            catch (IOException e) { die("Problem writing"); }
        });

        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToSout(String string){
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
        try{
            writer.write(string);
            writer.flush();
        } catch (IOException e){
            die("Problem writing");
        }
    }

    public void printInitMessage(String dataToMigrate){
        System.out.println("Migrating data " + dataToMigrate +
                " using Grakn Engine " + getEngineURI() +
                " into graph " + getKeyspace());
    }

    public void printWholeCompletionMessage(){
        System.out.println("Migration complete.");

        if(hasOption("v")) {
            System.out.println("Gathering information about migrated data. If in a hurry, you can ctrl+c now.");

            GraknGraph graph = getGraph();
            QueryBuilder qb = graph.graql();

            StringBuilder builder = new StringBuilder();
            builder.append("Graph ontology contains:\n");
            builder.append("\t ").append(graph.getMetaEntityType().instances().size()).append(" entity types\n");
            builder.append("\t ").append(graph.getMetaRelationType().instances().size()).append(" relation types\n");
            builder.append("\t ").append(graph.getMetaRoleType().instances().size()).append(" role types\n");
            builder.append("\t ").append(graph.getMetaResourceType().instances().size()).append(" resource types\n");
            builder.append("\t ").append(graph.getMetaRuleType().instances().size()).append(" rule types\n\n");

            builder.append("Graph data contains:\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("entity-type")).select("x").distinct().aggregate(count()).execute()).append(" entities\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("relation-type")).select("x").distinct().aggregate(count()).execute()).append(" relations\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("resource-type")).select("x").distinct().aggregate(count()).execute()).append(" resources\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("rule-type")).select("x").distinct().aggregate(count()).execute()).append(" rules\n\n");

            System.out.println(builder);

            graph.close();
        }
    }

    public void initiateShutdown(){
        System.out.println("Initiating shutdown...");
    }

    public String getEngineURI(){
        return cmd.getOptionValue("u", Grakn.DEFAULT_URI);
    }

    public String getKeyspace(){
        return cmd.getOptionValue("k", properties.getProperty(ConfigProperties.DEFAULT_KEYSPACE_PROPERTY));
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

    public String fileAsString(File file){
        try {
            return Files.readLines(file, StandardCharsets.UTF_8).stream().collect(joining("\n"));
        } catch (IOException e) {
            die("Could not read file " + file.getPath());
            throw new RuntimeException(e);
        }
    }

    public Loader getLoader(){
        return getEngineURI().equals(Grakn.DEFAULT_URI)
                ? new LoaderImpl(getKeyspace())
                : new LoaderClient(getKeyspace(), Collections.singleton(getEngineURI()));
    }

    public GraknGraph getGraph(){
        return Grakn.factory(getEngineURI(), getKeyspace()).getGraph();
    }

    public void addOptions(Options options) {
        options.getOptions().forEach(defaultOptions::addOption);
    }

    public String die(Throwable throwable){
        return die(ExceptionUtils.getFullStackTrace(throwable));
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
