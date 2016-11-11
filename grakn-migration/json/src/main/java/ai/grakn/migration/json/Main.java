/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.migration.json;

import ai.grakn.migration.base.io.MigrationLoader;
import ai.grakn.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;

import java.io.File;

/**
 * Main program to migrate a JSON schema and data into a Grakn graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Grakn graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Grakn engine.
 */
public class Main {

    private static Options options = new Options();
    static {
        options.addOption("i", "input", true, "input json data file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("b", "batch", true, "number of row to load at once");
    }

    public static void main(String[] args){
        MigrationCLI.create(args, options).ifPresent(Main::runJson);
    }

    public static void runJson(MigrationCLI cli){
        String jsonDataFileName = cli.getRequiredOption("input", "Data file missing (-i)");
        String jsonTemplateName = cli.getRequiredOption("template", "Template file missing (-t)");
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

        String template = cli.fileAsString(jsonTemplateFile);
        try(JsonMigrator jsonMigrator = new JsonMigrator(template, jsonDataFile)){

            if(cli.hasOption("n")){
                cli.writeToSout(jsonMigrator.migrate());
            } else {
                MigrationLoader.load(cli.getLoader(), batchSize, jsonMigrator);
                cli.printWholeCompletionMessage();
            }
        } catch (Throwable throwable){
            cli.die(throwable);
        }

        cli.initiateShutdown();
    }
}
