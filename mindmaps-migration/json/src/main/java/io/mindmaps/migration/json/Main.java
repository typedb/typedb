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

import com.google.common.io.Files;
import io.mindmaps.migration.base.io.MigrationCLI;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static io.mindmaps.migration.base.io.MigrationCLI.die;
import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate a JSON schema and data into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Mindmaps graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Mindmaps engine.
 */
public class Main {

    static {
        MigrationCLI.addOption("f", "file", true, "json data file");
        MigrationCLI.addOption("t", "template", true, "graql template to apply over data");
        MigrationCLI.addOption("b", "batch", true, "number of row to load at once");
    }

    public static void main(String[] args){

        MigrationCLI interpreter = new MigrationCLI(args);

        String jsonDataFileName = interpreter.getRequiredOption("f", "Data file missing (-f)");
        String jsonTemplateName = interpreter.getRequiredOption("t", "Template file missing (-t)");
        int batchSize = interpreter.hasOption("b") ? Integer.valueOf(interpreter.getOption("b")) : JsonMigrator.BATCH_SIZE;

        // get files
        File jsonDataFile = new File(jsonDataFileName);
        File jsonTemplateFile = new File(jsonTemplateName);

        if(!jsonDataFile.exists()){
            die("Cannot find file: " + jsonDataFileName);
        }

        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            die("Cannot find file: " + jsonTemplateName);
        }

        interpreter.printInitMessage(jsonDataFile.getPath());

        try{
            JsonMigrator migrator = new JsonMigrator(interpreter.getLoader())
                                        .setBatchSize(batchSize);

            String template = Files.readLines(jsonTemplateFile, StandardCharsets.UTF_8).stream().collect(joining("\n"));
            migrator.migrate(template, jsonDataFile);

            interpreter.printCompletionMessage();
        } catch (Throwable throwable){
            die(throwable.getMessage());
        }
    }
}
