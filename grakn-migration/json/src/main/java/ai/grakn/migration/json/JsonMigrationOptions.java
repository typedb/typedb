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

import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.base.MigrationOptions;

import static java.lang.Integer.parseInt;

/**
 * Configure the default JSON migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class JsonMigrationOptions extends MigrationOptions {

    private final String batch = Integer.toString(Migrator.BATCH_SIZE);
    private final String active = Integer.toString(Migrator.ACTIVE_TASKS);

    public JsonMigrationOptions(String[] args){
        super();

        options.addOption("i", "input", true, "Input json data file or directory.");
        options.addOption("t", "template", true, "Graql template to apply to the data.");
        options.addOption("b", "batch", true, "Number of rows to execute in one Grakn transaction. Default 25.");
        options.addOption("a", "active", true, "Number of tasks (batches) running on the server at any one time. Default 25.");

        parse(args);
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }

    public int getNumberActiveTasks() {
        return parseInt(command.getOptionValue("a", active));
    }
}
