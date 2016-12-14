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
import ai.grakn.engine.backgroundtasks.InMemoryTaskManager;
import com.google.common.io.Files;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.QueryBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.joining;

public class MigrationCLI {

    private static final String COULD_NOT_CONNECT  = "Could not connect to Grakn Engine. Have you run 'grakn.sh start'?";
    private static final ConfigProperties properties = ConfigProperties.getInstance();

    public static <T extends MigrationOptions> Optional<T> create(T options){
        try {
            if (options.isHelp()) {
                printHelpMessage(options);
            }

            if(options.getNumberOptions() == 0){
                printHelpMessage(options);
                throw new IllegalArgumentException("Helping");
            } else if(options.getNumberOptions() == 1 && options.isHelp()){
                throw new IllegalArgumentException("Helping");
            }

            if(!GraknEngineServer.isRunning()){
                System.out.println(COULD_NOT_CONNECT);
            }

            //noinspection unchecked
            return Optional.of(options);
        } catch (Throwable e){
            return Optional.empty();
        }
    }

    public static void writeToSout(Stream<InsertQuery> queries){
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

    public static void writeToSout(String string){
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
        try{
            writer.write(string);
            writer.flush();
        } catch (IOException e){
            die("Problem writing");
        }
    }

    public static void printInitMessage(MigrationOptions options, String dataToMigrate){
        System.out.println("Migrating data " + dataToMigrate +
                " using Grakn Engine " + options.getUri() +
                " into graph " + options.getKeyspace());
        System.out.println("See the Grakn engine logs for more detail about loading status and any resulting stacktraces: " + properties.getLogFilePath());
    }

    public static void printWholeCompletionMessage(MigrationOptions options){
        System.out.println("Migration complete.");

        if(options.isVerbose()) {
            System.out.println("Gathering information about migrated data. If in a hurry, you can ctrl+c now.");

            GraknGraph graph = Grakn.factory(options.getUri(), options.getKeyspace()).getGraph();
            QueryBuilder qb = graph.graql();

            StringBuilder builder = new StringBuilder();
            builder.append("Graph ontology contains:\n");
            builder.append("\t ").append(graph.admin().getMetaEntityType().instances().size()).append(" entity types\n");
            builder.append("\t ").append(graph.admin().getMetaRelationType().instances().size()).append(" relation types\n");
            builder.append("\t ").append(graph.admin().getMetaRoleType().instances().size()).append(" role types\n");
            builder.append("\t ").append(graph.admin().getMetaResourceType().instances().size()).append(" resource types\n");
            builder.append("\t ").append(graph.admin().getMetaRuleType().instances().size()).append(" rule types\n\n");

            builder.append("Graph data contains:\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("entity-type")).select("x").distinct().aggregate(count()).execute()).append(" entities\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("relation-type")).select("x").distinct().aggregate(count()).execute()).append(" relations\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("resource-type")).select("x").distinct().aggregate(count()).execute()).append(" resources\n");
            builder.append("\t ").append(qb.match(var("x").isa(var("y")), var("y").isa("rule-type")).select("x").distinct().aggregate(count()).execute()).append(" rules\n\n");

            System.out.println(builder);

            graph.close();
        }
    }

    public static void initiateShutdown(){
        System.out.println("Initiating shutdown...");
        InMemoryTaskManager.getInstance().shutdown();
        System.out.println("Completed shutdown...");
    }

    public static String fileAsString(File file){
        try {
            return Files.readLines(file, StandardCharsets.UTF_8).stream().collect(joining("\n"));
        } catch (IOException e) {
            die("Could not read file " + file.getPath());
            throw new RuntimeException(e);
        }
    }

    public static String die(Throwable throwable){
        return die(ExceptionUtils.getFullStackTrace(throwable));
    }

    public static String die(String errorMsg) {
        throw new RuntimeException(errorMsg);
    }

    public static void printHelpMessage(MigrationOptions options){
        HelpFormatter helpFormatter = new HelpFormatter();
        PrintWriter printWriter = new PrintWriter(System.out);
        int width = helpFormatter.getWidth();
        int leftPadding = helpFormatter.getLeftPadding();
        int descPadding = helpFormatter.getDescPadding();
        helpFormatter.printHelp(printWriter, width, "migration.sh", null, options.getOptions(), leftPadding, descPadding, null);
        printWriter.flush();
    }
}
