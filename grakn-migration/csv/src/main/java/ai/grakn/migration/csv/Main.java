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

package ai.grakn.migration.csv;

import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.migration.base.io.MigrationCLI;
import ai.grakn.migration.base.io.MigrationLoader;

import java.io.File;
import java.util.Optional;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static ai.grakn.migration.base.io.MigrationCLI.fileAsString;
import static ai.grakn.migration.base.io.MigrationCLI.initiateShutdown;
import static ai.grakn.migration.base.io.MigrationCLI.printInitMessage;
import static ai.grakn.migration.base.io.MigrationCLI.printWholeCompletionMessage;
import static ai.grakn.migration.base.io.MigrationCLI.writeToSout;

/**
 * Main program to migrate CSV files into a Grakn graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
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
        MigrationCLI.init(args, CSVMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach((options) -> runCSV(finalManager, options));
    }

    public static void runCSV(ClusterManager manager, CSVMigrationOptions options){
        // get files
        File csvDataFile = new File(options.getInput());
        File csvTemplate = new File(options.getTemplate());

        if (!csvTemplate.exists()) {
            die("Cannot find file: " + options.getTemplate());
        }

        if (!csvDataFile.exists()) {
            die("Cannot find file: " + options.getInput());
        }

        printInitMessage(options, csvDataFile.getPath());

        String template = fileAsString(csvTemplate);
        try (
                CSVMigrator csvMigrator =
                        new CSVMigrator(template, csvDataFile)
                                .setSeparator(options.getSeparator())
                                .setQuoteChar(options.getQuote())
                                .setNullString(options.getNullString())
        ) {

            if (options.isNo()) {
                writeToSout(csvMigrator.migrate());
            } else {
                MigrationLoader.load(manager, options.getKeyspace(), options.getBatch(), csvMigrator);
                printWholeCompletionMessage(options);
            }
        } catch (Throwable throwable) {
            die(throwable);
        }

        initiateShutdown();
    }
}
