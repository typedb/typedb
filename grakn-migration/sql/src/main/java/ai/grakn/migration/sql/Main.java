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

import ai.grakn.migration.base.AbstractMigrator;
import ai.grakn.migration.base.io.MigrationCLI;
import ai.grakn.migration.base.io.MigrationLoader;
import org.apache.commons.cli.Options;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Main program to migrate an SQL database to a Grakn graph.
 * Expected arguments include the JDBC driver information and template.
 */
public class Main {

    private static Options options = new Options();
    static {
        options.addOption("driver", true, "JDBC driver");
        options.addOption("location", true, "JDBC url (location of DB)");
        options.addOption("user", true, "JDBC username");
        options.addOption("pass", true, "JDBC password");
        options.addOption("q", "query", true, "SQL Query");
        options.addOption("t", "template", true, "template for the given SQL query");
    }

    public static void main(String[] args){
        MigrationCLI.create(args, options).ifPresent(Main::runSQL);
    }

    public static void runSQL(MigrationCLI cli){
        String jdbcDriver = cli.getRequiredOption("driver", "No driver specified (-driver)");
        String jdbcDBUrl = cli.getRequiredOption("location", "No db specified (-location)");
        String jdbcUser = cli.getRequiredOption("user", "No username specified (-user)");
        String jdbcPass = cli.getRequiredOption("pass", "No password specified (-pass)");
        String sqlQuery = cli.getRequiredOption("query", "No SQL query specified (-query)");
        String sqlTemplateName = cli.getRequiredOption("template", "Template file missing (-t)");
        int batchSize = cli.hasOption("b") ? Integer.valueOf(cli.getOption("b")) : AbstractMigrator.BATCH_SIZE;

        File sqlTemplate = new File(sqlTemplateName);

        if(!sqlTemplate.exists()){
            cli.die("Cannot find file: " + sqlTemplateName);
        }

        cli.printInitMessage(jdbcDBUrl + " using " + sqlQuery);

        String template = cli.fileAsString(sqlTemplate);
        try(Connection connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass)) {

            SQLMigrator sqlMigrator = new SQLMigrator(sqlQuery, template, connection);

            if(cli.hasOption("n")){
                cli.writeToSout(sqlMigrator.migrate());
            } else {
                MigrationLoader.load(cli.getLoader(), batchSize, sqlMigrator);
                cli.printWholeCompletionMessage();
            }

        } catch (Throwable throwable){
            cli.die(throwable);
        }

        cli.initiateShutdown();
    }
}
