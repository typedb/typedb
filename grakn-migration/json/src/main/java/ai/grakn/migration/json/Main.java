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

import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.migration.base.io.MigrationLoader;
import ai.grakn.migration.base.io.MigrationCLI;

import java.io.File;
import java.util.Optional;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static ai.grakn.migration.base.io.MigrationCLI.fileAsString;
import static ai.grakn.migration.base.io.MigrationCLI.initiateShutdown;
import static ai.grakn.migration.base.io.MigrationCLI.printInitMessage;
import static ai.grakn.migration.base.io.MigrationCLI.printWholeCompletionMessage;
import static ai.grakn.migration.base.io.MigrationCLI.writeToSout;

/**
 * Main program to migrate a JSON schema and data into a Grakn graph. For use from a command line.
 * Expected arguments are the JSON schema files and the Grakn graph name.
 * Additionally, JSON data file or directory of files may be provided as well as the URL of Grakn engine.
 *
 * @author alexandraorth
 */
public class Main {

    public static void main(String[] args) {
        start(null, args);
    }

    public static void start(ClusterManager manager, String[] args){
        if(manager == null){
            manager = new ClusterManager();
        }

        ClusterManager finalManager = manager;
        MigrationCLI.init(args, JsonMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach((options) -> runJson(finalManager, options));
    }

    public static void runJson(ClusterManager manager, JsonMigrationOptions options){
        File jsonDataFile = new File(options.getInput());
        File jsonTemplateFile = new File(options.getTemplate());

        if(!jsonDataFile.exists()){
            die("Cannot find file: " + options.getInput());
        }

        if(!jsonTemplateFile.exists() || jsonTemplateFile.isDirectory()){
            die("Cannot find file: " + options.getTemplate());
        }

        printInitMessage(options, jsonDataFile.getPath());

        String template = fileAsString(jsonTemplateFile);
        try(JsonMigrator jsonMigrator = new JsonMigrator(template, jsonDataFile)){

            if(options.isNo()){
                writeToSout(jsonMigrator.migrate());
            } else {
                MigrationLoader.load(manager, options.getKeyspace(), options.getBatch(), jsonMigrator);
                printWholeCompletionMessage(options);
            }
        } catch (Throwable throwable){
            die(throwable);
        }

        initiateShutdown();
    }
}
