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
import io.mindmaps.migration.base.AbstractMigrator;
import io.mindmaps.migration.base.LoadingMigrator;
import io.mindmaps.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.joining;

/**
 * Main program to migrate a JSON schema and data into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Mindmaps graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Mindmaps engine.
 */
public class Main {

    private static Options options = new Options();
    static {
        options.addOption("f", "file", true, "json data file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("b", "batch", true, "number of row to load at once");
    }

    public static void main(String[] args){

        MigrationCLI cli = new MigrationCLI(args, options);

        String jsonDataFileName = cli.getRequiredOption("f", "Data file missing (-f)");
        String jsonTemplateName = cli.getRequiredOption("t", "Template file missing (-t)");
        int batchSize = cli.hasOption("b") ? Integer.valueOf(cli.getOption("b")) : JsonMigrator.BATCH_SIZE;

        // get files
        File jsonDataFile = new File(jsonDataFileName);
        File jsonTemplateFile = new File(jsonTemplateName);

        if(!jsonDataFile.exists()){
            cli.die("Cannot find file: " + jsonDataFileName);
        }

        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            cli.die("Cannot find file: " + jsonTemplateName);
        }

        cli.printInitMessage(jsonDataFile.getPath());

        try{
            String template = Files.readLines(jsonTemplateFile, StandardCharsets.UTF_8).stream().collect(joining("\n"));

            LoadingMigrator migrator = new JsonMigrator()
                    .getLoadingMigrator(cli.getLoader())
                    .setBatchSize(batchSize);

            migrator.migrate(template, jsonDataFile);

            cli.printWholeCompletionMessage();
        } catch (Throwable throwable){
            cli.die(throwable.getMessage());
        }
    }
}
