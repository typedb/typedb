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
import io.mindmaps.migration.base.io.MigrationCLI;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static io.mindmaps.migration.base.io.MigrationCLI.die;
import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate CSV files into a Mindmaps graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
 */
public class Main {

    static {
        MigrationCLI.addOption("f", "file", true, "csv file");
        MigrationCLI.addOption("t", "template", true, "graql template to apply over data");
        MigrationCLI.addOption("d", "delimiter", true, "delimiter of columns in input file");
        MigrationCLI.addOption("b", "batch", true, "number of row to load at once");
    }

    public static void main(String[] args){

        MigrationCLI interpreter = new MigrationCLI(args);

        String csvDataFileName = interpreter.getRequiredOption("f", "Data file missing (-f)");
        String csvTemplateName = interpreter.getRequiredOption("t", "Template file missing (-t)");
        char csvDelimiter =  interpreter.hasOption("d") ? interpreter.getOption("d").charAt(0) : CSVMigrator.DELIMITER;
        int batchSize = interpreter.hasOption("b") ? Integer.valueOf(interpreter.getOption("b")) : CSVMigrator.BATCH_SIZE;

        // get files
        File csvDataFile = new File(csvDataFileName);
        File csvTemplate = new File(csvTemplateName);

        if(!csvTemplate.exists() || !csvDataFile.exists()){
            die("Cannot find file: " + csvDataFileName);
        }

        interpreter.printInitMessage(csvDataFile.getPath());

        try{
            CSVMigrator migrator = new CSVMigrator(interpreter.getLoader())
                                        .setDelimiter(csvDelimiter)
                                        .setBatchSize(batchSize);

            String template = Files.readLines(csvTemplate, StandardCharsets.UTF_8).stream().collect(joining("\n"));
            migrator.migrate(template, csvDataFile);

            interpreter.printCompletionMessage();
        }
        catch (Throwable throwable){
            die(throwable.getMessage());
        }
    }
}
