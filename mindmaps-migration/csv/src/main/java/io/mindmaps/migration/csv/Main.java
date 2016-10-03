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
import com.google.common.io.Files;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate CSV files into a Mindmaps graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
 */
public class Main {

    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: ./migration.sh csv -file <csv file> -template <template file> [-delimiter <delimiter>] [-batch <number of rows>] [-graph <graph name>] [-engine <Mindmaps engine URL>])");
        System.exit(-1);
    }

    public static void main(String[] args){

        String batchSize = null;
        String csvFileName = null;
        String csvTemplateName = null;
        String csvDelimiter = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-file".equals(args[i]))
                csvFileName = args[++i];
            else if ("-template".equals(args[i]))
                csvTemplateName = args[++i];
            else if ("-delimiter".equals(args[i]))
                csvDelimiter = args[++i];
            else if ("-batch".equals(args[i]))
                batchSize = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if("csv".equals(args[0]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        if(csvFileName == null){
            die("Please specify CSV file using the -file option");
        }
        File csvFile = new File(csvFileName);

        if(csvTemplateName == null){
            die("Please specify the template using the -template option");
        }
        File csvTemplate = new File(csvFileName);

        if(!csvTemplate.exists() || !csvFile.exists()){
            die("Cannot find file: " + csvFileName);
        }


        if(graphName == null){
            graphName = csvFile.getName().replace(".", "_");
        }

        System.out.println("Migrating " + csvFileName + " using MM Engine " +
                (engineURL == null ? "local" : engineURL ) + " into graph " + graphName);

        //
        try{
            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            CSVMigrator migrator = new CSVMigrator(loader)
                                        .setDelimiter(csvDelimiter == null ? CSVMigrator.DELIMITER : csvDelimiter.charAt(0))
                                        .setBatchSize(batchSize == null ? CSVMigrator.BATCH_SIZE : Integer.valueOf(batchSize));

            String template = Files.readLines(csvTemplate, StandardCharsets.UTF_8).stream().collect(joining("\n"));

            migrator.migrate(template, csvFile);

            System.out.println("Migration complete!");
        }
        catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

        System.exit(0);
    }
}
