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

package ai.grakn.migration.csv;

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
 * Main program to migrate CSV files into a Grakn graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
 */
public class Main {

    private static ConfigProperties properties = ConfigProperties.getInstance();

    static void die(String errorMsg) {
        throw new RuntimeException(errorMsg + "\nSyntax: ./migration.sh csv -file <csv file> -template <template file> [-delimiter <delimiter>] [-batch <number of rows>] [-graph <graph name>] [-engine <Grakn engine URL>])");
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
            else if(i == 0 && "csv".equals(args[i]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        if(csvFileName == null){
            die("Please specify CSV file using the -csv option");
        }
        File csvFile = new File(csvFileName);

        if(csvTemplateName == null){
            die("Please specify the template using the -template option");
        }
        File csvTemplate = new File(csvTemplateName);

        if(!csvTemplate.exists() || !csvFile.exists()){
            die("Cannot find file: " + csvFileName);
        }


        if(graphName == null){
            graphName = properties.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
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
            die(throwable.getMessage());
        }
    }
}
