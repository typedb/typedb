/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

/*-
 * #%L
 * migration-sql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.migration.base.MigrationOptions;
import java.sql.Driver;

/**
 * Configure the default SQL migration options and access arguments passed by the user
 * @author alexandraorth
 */
public class SQLMigrationOptions extends MigrationOptions {

    public SQLMigrationOptions(String[] args){
        super();

        options.addOption("driver", true, "JDBC driver");
        options.addOption("location", true, "JDBC url (location of DB)");
        options.addOption("user", true, "JDBC username");
        options.addOption("pass", true, "JDBC password");
        options.addOption("q", "query", true, "SQL Query");
        options.addOption("t", "template", true, "Graql template to apply to the data.");
        parse(args);
    }

    public Driver getDriver() {
        if(command.hasOption("driver")){
            try {
                return (Driver) Class.forName(command.getOptionValue("driver")).newInstance();
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        throw new IllegalArgumentException("No driver specified (-driver)");
    }

    public boolean hasDriver(){
        return command.hasOption("driver");
    }

    public String getLocation() {
        if(command.hasOption("location")){
            return command.getOptionValue("location");
        }

        throw new IllegalArgumentException("No db specified (-location)");
    }

    public String getUsername() {
        if(command.hasOption("user")){
            return command.getOptionValue("user");
        }

        throw new IllegalArgumentException("No username specified (-user)");
    }

    public String getPassword() {
        if(command.hasOption("pass")){
            return command.getOptionValue("pass");
        }

        throw new IllegalArgumentException("No password specified (-pass)");
    }

    public String getQuery() {
        if(command.hasOption("query")){
            return command.getOptionValue("query");
        }

        throw new IllegalArgumentException("No SQL query specified (-query)");
    }
}
