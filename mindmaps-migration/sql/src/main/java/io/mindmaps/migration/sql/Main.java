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

package io.mindmaps.migration.sql;

import io.mindmaps.migration.base.io.MigrationCLI;
import org.apache.commons.cli.Options;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Main program to migrate a SQL database into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JDBC driver, JDBC username, JDBC password, JDBC database name and the Mindmaps Graph name
 * Optionally you can provide the Mindmaps engine URL
 */
public class Main {

    private static Options getOptions(){
        Options options = new Options();
        options.addOption("driver", "driver", true, "JDBC driver");
        options.addOption("db", "database", true, "URL to SQL database");
        options.addOption("user", "user", true, "Username to access SQL database");
        options.addOption("pass", "pass", true, "Password to access SQL database");
        return options;
    }

    public static void main(String[] args){
        MigrationCLI cli = new MigrationCLI(args, getOptions());

        String jdbcDriver = cli.getRequiredOption("driver", "No driver specified (-driver)");
        String jdbcDBUrl = cli.getRequiredOption("db", "No db specified (-database)");
        String jdbcUser = cli.getRequiredOption("user", "No username specified (-user)");
        String jdbcPass = cli.getRequiredOption("pass", "No password specified (-pass)");

        cli.printInitMessage(jdbcDBUrl);

        // perform migration
        SQLSchemaMigrator schemaMigrator = new SQLSchemaMigrator();
        SQLDataMigrator dataMigrator = new SQLDataMigrator();

        try{
            // make JDBC connection
            Class.forName(jdbcDriver).newInstance();
            Connection connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass);

            schemaMigrator
                    .configure(connection)
                    .migrate(cli.getLoader())
                    .close();

            connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass);
            dataMigrator
                    .configure(connection)
                    .migrate(cli.getLoader())
                    .close();

            cli.printWholeCompletionMessage();
        }
        catch (Throwable throwable){
           cli.die(throwable.getMessage());
        }
    }

}
