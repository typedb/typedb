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

package io.mindmaps.migration.csv;

import com.google.common.collect.Lists;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;

import java.io.File;

/**
 * Main program to migrate CSV files into a Mindmaps graph. For use from a command line.
 * Expected arguments are the CSV file and the Mindmaps graph name.
 * Additionally, name of CSV entity and url of Mindmaps engine can be provided.
 */
public class Main {

    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: ./migration.sh csv -file <csv filename> -graph <graph name> [-engine <Mindmaps engine URL>] [-as <name of this entity type>]");
        System.exit(-1);
    }

    public static void main(String[] args){

        String csvFileName = null;
        String csvEntityType = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-file".equals(args[i]))
                csvFileName = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if ("-as".equals(args[i])){
                csvEntityType = args[++i];
            }
            else if("csv".equals(args[0])) {
                continue;
            }
            else
                die("Unknown option " + args[i]);
        }

        if(csvFileName == null){
            die("Please specify CSV file using the -csv option");
        }
        File csvFile = new File(csvFileName);
        if(!csvFile.exists()){
            die("Cannot find file: " + csvFileName);
        }
        if(graphName == null){
            die("Please provide the name of the graph using -graph");
        }
        if(csvEntityType == null){
            csvEntityType = csvFile.getName().replaceAll("[^A-Za-z0-9]", "_");
        }

        System.out.println("Migrating " + csvFileName + " using MM Engine " +
                (engineURL == null ? "local" : engineURL ) + " into graph " + graphName);


        //
        try{
            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

//            CSVMigrator migrator = new CSVMigrator(loader, batchSize);



        }
        catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

        System.exit(0);
    }
}
