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

package io.mindmaps.migration.json;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate a JSON schema and data into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Mindmaps graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Mindmaps engine.
 */
public class Main {

    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: ./migration.sh json -schema <schema filename> -data <data filename or dir> -template <template file> [-graph <graph name>] [-batch <number of rows>] [-engine <Mindmaps engine URL>]");
        System.exit(-1);
    }

    public static void main(String[] args){

        String batchSize = null;
        String jsonTemplateName = null;
        String jsonSchemaFileName = null;
        String jsonDataFileName = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-schema".equals(args[i]))
                jsonSchemaFileName = args[++i];
            else if ("-data".equals(args[i]))
                jsonDataFileName = args[++i];
            else if ("-template".equals(args[i]))
                jsonTemplateName = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if ("-batch".equals(args[i]))
                batchSize = args[++i];
            else if("json".equals(args[0]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        // check for arguments
        if(graphName == null){
            die("Please provide the name of the graph using -graph");
        }
        if(jsonSchemaFileName == null){
            die("Please specify CSV file using the -csv option");
        }
        if(jsonDataFileName == null){
            die("Please specify CSV file using the -csv option");
        }

        // get files
        File jsonSchemaFile = new File(jsonSchemaFileName);
        if(!jsonSchemaFile.exists() || jsonSchemaFile.isDirectory()){
            die("Cannot find file: " + jsonSchemaFileName);
        }

        File jsonDataFile = new File(jsonDataFileName);
        if(!jsonDataFile.exists()){
            die("Cannot find file: " + jsonDataFileName);
        }

        File jsonTemplateFile = new File(jsonTemplateName);
        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            die("Cannot find file: " + jsonTemplateName);
        }

        System.out.println("Migrating schema " + jsonSchemaFileName +
                " and data " + jsonDataFileName +
                " using MM Engine " + (engineURL == null ? "local" : engineURL ) +
                " into graph " + graphName);

        try{
            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            JsonMigrator migrator = new JsonMigrator(loader)
                    .setBatchSize(batchSize == null ? JsonMigrator.BATCH_SIZE : Integer.valueOf(batchSize));

            String template = Files.readLines(jsonTemplateFile, StandardCharsets.UTF_8).stream().collect(joining("\n"));

            migrator.migrate(template, jsonDataFile);


            System.out.println("DataType migration successful");

        } catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

    }
}
