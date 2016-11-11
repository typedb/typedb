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

import ai.grakn.migration.base.AbstractMigrator;
import ai.grakn.migration.base.io.MigrationCLI;
import ai.grakn.migration.base.io.MigrationLoader;
import org.apache.commons.cli.Options;

import java.io.File;

/**
 * Main program to migrate CSV files into a Grakn graph. For use from a command line.
 * Expected arguments are the CSV file and the Graql template.
 * Additionally, delimiter, batch size, location of engine and graph name can be provided.
 */
public class Main {

    private static Options getOptions(){
        Options options = new Options();
        options.addOption("i", "input", true, "input csv file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("s", "separator", true, "separator of columns in input file");
        options.addOption("b", "batch", true, "number of row to load at once");
        return options;
    }

    public static void main(String[] args) {
        MigrationCLI.create(args, getOptions()).ifPresent(Main::runCSV);
    }

    public static void runCSV(MigrationCLI cli){
        String csvDataFileName = cli.getRequiredOption("input", "Data file missing (-i)");
        String csvTemplateName = cli.getRequiredOption("template", "Template file missing (-t)");
        int batchSize = cli.hasOption("b") ? Integer.valueOf(cli.getOption("b")) : AbstractMigrator.BATCH_SIZE;
        String delimiterString = cli.hasOption("s") ? cli.getOption("s") : Character.toString(CSVMigrator.SEPARATOR);

        if (delimiterString.toCharArray().length != 1) {
            cli.die("Wrong number of characters in delimiter " + delimiterString);
        }

        char csvDelimiter = delimiterString.toCharArray()[0];

        // get files
        File csvDataFile = new File(csvDataFileName);
        File csvTemplate = new File(csvTemplateName);

        if (!csvTemplate.exists()) {
            cli.die("Cannot find file: " + csvTemplateName);
        }

        if (!csvDataFile.exists()) {
            cli.die("Cannot find file: " + csvDataFileName);
        }

        cli.printInitMessage(csvDataFile.getPath());

        String template = cli.fileAsString(csvTemplate);
        try (
                CSVMigrator csvMigrator =
                        new CSVMigrator(template, csvDataFile).setSeparator(csvDelimiter)
        ) {

            if (cli.hasOption("n")) {
                cli.writeToSout(csvMigrator.migrate());
            } else {
                MigrationLoader.load(cli.getLoader(), batchSize, csvMigrator);
                cli.printWholeCompletionMessage();
            }
        } catch (Throwable throwable) {
            cli.die(throwable);
        }

        cli.initiateShutdown();
    }
}
