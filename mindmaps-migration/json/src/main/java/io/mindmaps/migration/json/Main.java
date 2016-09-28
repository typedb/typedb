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

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import mjson.Json;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.MalformedURLException;

/**
 * Main program to migrate a JSON schema and data into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Mindmaps graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Mindmaps engine.
 */
public class Main {

    public static final String[] FILE_TYPES = {"json"};

    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: ./migration.sh json -schema <schema filename or dir> -graph <graph name> [-engine <Mindmaps engine URL>] [-data <data filename or dir>]");
        System.exit(-1);
    }

    public static void main(String[] args){

        String jsonSchemaFileName = null;
        String jsonDataFileName = null;
        String entityType = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-schema".equals(args[i]))
                jsonSchemaFileName = args[++i];
            else if ("-data".equals(args[i]))
                jsonDataFileName = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if ("-type".equals(args[i]))
                entityType = args[++i];
            else if("json".equals(args[0])) {
                continue;
            }
            else
                die("Unknown option " + args[i]);
        }

        if(graphName == null){
            die("Please provide the name of the graph using -graph");
        }
        if(jsonSchemaFileName == null){
            die("Please specify CSV file using the -csv option");
        }

        File jsonSchemaFile = new File(jsonSchemaFileName);
        if(!jsonSchemaFile.exists()){
            die("Cannot find file: " + jsonSchemaFileName);
        }

        if(jsonDataFileName != null && entityType == null){
            die("Cannot migrate data without providing an entity type");
        }

        File jsonDataFile = null;
        if(jsonDataFileName != null) {
            jsonDataFile = new File(jsonDataFileName);
        }

        System.out.println("Migrating schema " + jsonSchemaFileName +
                " and data " + jsonDataFileName +
                " using MM Engine " + (engineURL == null ? "local" : engineURL ) +
                " into graph " + graphName);

        final String finalEntityType = entityType;

        JsonSchemaMigrator schemaMigrator = new JsonSchemaMigrator();
        JsonDataMigrator dataMigrator = new JsonDataMigrator();

        try{
            MindmapsGraph graph = engineURL == null ? Mindmaps.factory(Mindmaps.DEFAULT_URI, graphName).getGraph()
                                                    : Mindmaps.factory(engineURL, graphName).getGraph();

            if(jsonSchemaFile.isDirectory()){
                System.out.println(FileUtils.listFiles(jsonSchemaFile, FILE_TYPES, true));
                FileUtils.listFiles(jsonSchemaFile, FILE_TYPES, true).stream()
                        .forEach(f -> migrateSchema(schemaMigrator, graph, f, finalEntityType));
            } else {
                migrateSchema(schemaMigrator, graph, jsonSchemaFile, entityType);
            }

            graph.commit();

            System.out.println("Schema migration successful");

            if(jsonDataFile == null){
                System.exit(0);
            }

            if(jsonDataFile.isDirectory()){
                FileUtils.listFiles(jsonDataFile, FILE_TYPES, true).stream()
                        .forEach(f -> migrateData(dataMigrator, graph, f, finalEntityType));
            } else {
                migrateData(dataMigrator, graph, jsonDataFile, entityType);
            }

            graph.commit();

            System.out.println("DataType migration successful");

        } catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

    }

    private static void migrateSchema(JsonSchemaMigrator migrator, MindmapsGraph graph, File file, String type) {
        System.out.println(file.getPath());
        migrator.graph(graph);
        if (type == null) {
            migrator.migrateSchema(Json.schema(file.toURI()));
        } else {
            migrator.migrateSchema(type, Json.schema(file.toURI()));
        }
    }

    private static void migrateData(JsonDataMigrator migrator, MindmapsGraph graph, File file, String type) {
        try {
            migrator
                    .graph(graph)
                    .migrateData(type, Json.read(file.toURI().toURL()));
        }
        catch (MalformedURLException e){
            e.printStackTrace(System.err);
        }
    }
}
