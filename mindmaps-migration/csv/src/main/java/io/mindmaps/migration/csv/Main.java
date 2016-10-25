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

import com.google.common.io.Files;
import io.mindmaps.migration.base.AbstractMigrator;
import io.mindmaps.migration.base.LoadingMigrator;
import io.mindmaps.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate CSV files into a Mindmaps graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
 */
public class Main {

    private static Options getOptions(){
        Options options = new Options();
        options.addOption("f", "file", true, "csv file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("d", "delimiter", true, "delimiter of columns in input file");
        options.addOption("b", "batch", true, "number of row to load at once");
        return options;
    }

    public static void main(String[] args){

        MigrationCLI cli = new MigrationCLI(args, getOptions());

        String csvDataFileName = cli.getRequiredOption("f", "Data file missing (-f)");
        String csvTemplateName = cli.getRequiredOption("t", "Template file missing (-t)");
        char csvDelimiter =  cli.hasOption("d") ? cli.getOption("d").charAt(0) : CSVMigrator.DELIMITER;
        int batchSize = cli.hasOption("b") ? Integer.valueOf(cli.getOption("b")) : CSVMigrator.BATCH_SIZE;

        // get files
        File csvDataFile = new File(csvDataFileName);
        File csvTemplate = new File(csvTemplateName);

        if(!csvTemplate.exists() || !csvDataFile.exists()){
            cli.die("Cannot find file: " + csvDataFileName);
        }

        cli.printInitMessage(csvDataFile.getPath());

        try{
            LoadingMigrator migrator = new CSVMigrator()
                                        .setDelimiter(csvDelimiter)
                                        .setBatchSize(batchSize)
                                        .getLoadingMigrator(cli.getLoader());

            String template = Files.readLines(csvTemplate, StandardCharsets.UTF_8).stream().collect(joining("\n"));
            migrator.migrate(template, csvDataFile);

            cli.printCompletionMessage();
        }
        catch (Throwable throwable){
            cli.die(throwable.getMessage());
        }
    }
}
