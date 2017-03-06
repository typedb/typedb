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

package ai.grakn.migration.sql;

import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.base.MigrationOptions;

import static ai.grakn.migration.base.MigrationCLI.die;
import static java.lang.Integer.parseInt;

/**
 * Configure the default SQL migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class SQLMigrationOptions extends MigrationOptions {

    private final String batch = Integer.toString(Migrator.BATCH_SIZE);
    private final String active = Integer.toString(Migrator.ACTIVE_TASKS);

    public SQLMigrationOptions(String[] args){
        super();

        options.addOption("driver", true, "JDBC driver");
        options.addOption("location", true, "JDBC url (location of DB)");
        options.addOption("user", true, "JDBC username");
        options.addOption("pass", true, "JDBC password");
        options.addOption("q", "query", true, "SQL Query");
        options.addOption("t", "template", true, "Graql template to apply to the data.");
        options.addOption("b", "batch", true, "Number of rows to execute in one Grakn transaction. Default 25.");
        options.addOption("a", "active", true, "Number of tasks (batches) running on the server at any one time. Default 25.");

        parse(args);
    }

    public String getDriver() {
        if(command.hasOption("driver")){
            return command.getOptionValue("driver");
        }
        return die("No driver specified (-driver)");
    }

    public String getLocation() {
        if(command.hasOption("location")){
            return command.getOptionValue("location");
        }
        return die("No db specified (-location)");
    }

    public String getUsername() {
        if(command.hasOption("user")){
            return command.getOptionValue("user");
        }
        return die("No username specified (-user)");
    }

    public String getPassword() {
        if(command.hasOption("pass")){
            return command.getOptionValue("pass");
        }
        return die("No password specified (-pass)");
    }

    public String getQuery() {
        if(command.hasOption("query")){
            return command.getOptionValue("query");
        }
        return die("No SQL query specified (-query)");
    }

    public int getBatch() {
        return parseInt(command.getOptionValue("b", batch));
    }

    public int getNumberActiveTasks() {
        return parseInt(command.getOptionValue("a", active));
    }
}
