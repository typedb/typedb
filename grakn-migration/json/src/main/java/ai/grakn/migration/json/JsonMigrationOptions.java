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

import ai.grakn.migration.base.AbstractMigrator;
import ai.grakn.migration.base.io.MigrationOptions;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static java.lang.Integer.parseInt;

/**
 * Configure the default JSON migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class JsonMigrationOptions extends MigrationOptions {

    private final String batch = Integer.toString(AbstractMigrator.BATCH_SIZE);

    public JsonMigrationOptions(String[] args){
        super(args);

        options.addOption("i", "input", true, "input json data file");
        options.addOption("t", "template", true, "graql template to apply over data");
        options.addOption("b", "batch", true, "number of row to load at once");

        parse(args);
    }

    public String getInput() {
        if(!command.hasOption("i")){
            die("Data file missing (-i)");
        }
        return command.getOptionValue("i");
    }

    public String getTemplate() {
        if(!command.hasOption("t")){
            die("Template file missing (-t)");
        }
        return command.getOptionValue("t");
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }
}
