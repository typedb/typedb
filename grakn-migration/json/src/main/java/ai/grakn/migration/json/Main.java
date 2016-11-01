/*
 * GraknDB - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Research Ltd
 *
 * GraknDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GraknDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GraknDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.migration.json;

import ai.grakn.engine.loader.BlockingLoader;
import ai.grakn.engine.loader.Loader;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import ai.grakn.engine.loader.BlockingLoader;
import ai.grakn.engine.loader.DistributedLoader;
import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.util.ConfigProperties;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate a JSON schema and data into a Grakn graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Grakn graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Grakn engine.
 */
public class Main {

    private static ConfigProperties properties = ConfigProperties.getInstance();

    static void die(String errorMsg) {
        throw new RuntimeException(errorMsg + "\nSyntax: ./migration.sh json -data <data filename or dir> -template <template file> [-graph <graph name>] [-batch <number of rows>] [-engine <Grakn engine URL>]");
    }

    public static void main(String[] args){

        String batchSize = null;
        String jsonTemplateName = null;
        String jsonDataFileName = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-data".equals(args[i]))
                jsonDataFileName = args[++i];
            else if ("-template".equals(args[i]))
                jsonTemplateName = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if ("-batch".equals(args[i]))
                batchSize = args[++i];
            else if(i == 0 && "json".equals(args[i]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        // check for arguments
        if(jsonDataFileName == null){
            die("Please specify JSON data file or dir using the -data option");
        }
        if(jsonTemplateName == null){
            die("Please specify Graql template using the -template option");
        }

        // get files
        File jsonDataFile = new File(jsonDataFileName);
        if(!jsonDataFile.exists()){
            die("Cannot find file: " + jsonDataFileName);
        }

        File jsonTemplateFile = new File(jsonTemplateName);
        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            die("Cannot find file: " + jsonTemplateName);
        }

        if(graphName == null){
            graphName = properties.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        }

        System.out.println("Migrating data " + jsonDataFileName +
                " using MM Engine " + (engineURL == null ? "local" : engineURL ) +
                " into graph " + graphName);

        try{
            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            JsonMigrator migrator = new JsonMigrator(loader)
                    .setBatchSize(batchSize == null ? JsonMigrator.BATCH_SIZE : Integer.valueOf(batchSize));

            String template = Files.readLines(jsonTemplateFile, StandardCharsets.UTF_8).stream().collect(joining("\n"));

            migrator.migrate(template, jsonDataFile);

            System.out.println("Data migration successful");
        } catch (Throwable throwable){
            die(throwable.getMessage());
        }
    }
}
