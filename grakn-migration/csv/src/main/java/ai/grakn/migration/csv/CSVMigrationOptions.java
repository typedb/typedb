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
import ai.grakn.migration.base.io.MigrationOptions;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static java.lang.Integer.parseInt;

/**
 * Configure the default CSV migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class CSVMigrationOptions extends MigrationOptions {
    private final String batch = Integer.toString(AbstractMigrator.BATCH_SIZE);
    private final String separator = Character.toString(CSVMigrator.SEPARATOR);
    private final String quote = Character.toString(CSVMigrator.QUOTE);
    private final String nullString = CSVMigrator.NULL_STRING;

    public CSVMigrationOptions(String[] args) {
        super(args);

        options.addOption("i", "input", true, "input csv file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("s", "separator", true, "separator of columns in input file");
        options.addOption("q", "quote", true, "character used to encapsulate values containing special characters");
        options.addOption("l", "null", true, "string that will be evaluated as null");
        options.addOption("b", "batch", true, "number of row to load at once");

        parse(args);
    }

    public char getSeparator() {
        String sep = command.getOptionValue("s", separator);
        if (sep.toCharArray().length != 1) {
            die("Wrong number of characters in quote " + sep);
        }

        return sep.charAt(0);
    }

    public char getQuote() {
        String quo = command.getOptionValue("q", quote);
        if (quo.toCharArray().length != 1) {
            die("Wrong number of characters in quote " + quo);
        }

        return quo.charAt(0);
    }

    public String getNullString() {
        return command.getOptionValue("l", nullString);
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }
}
