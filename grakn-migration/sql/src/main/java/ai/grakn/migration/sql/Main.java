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

import ai.grakn.engine.backgroundtasks.distributed.ClusterManager;
import ai.grakn.migration.base.io.MigrationCLI;
import ai.grakn.migration.base.io.MigrationLoader;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Optional;

import static ai.grakn.migration.base.io.MigrationCLI.die;
import static ai.grakn.migration.base.io.MigrationCLI.fileAsString;
import static ai.grakn.migration.base.io.MigrationCLI.initiateShutdown;
import static ai.grakn.migration.base.io.MigrationCLI.printInitMessage;
import static ai.grakn.migration.base.io.MigrationCLI.printWholeCompletionMessage;
import static ai.grakn.migration.base.io.MigrationCLI.writeToSout;

/**
 * Main program to migrate an SQL database to a Grakn graph.
 * Expected arguments include the JDBC driver information and template.
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
        MigrationCLI.init(args, SQLMigrationOptions::new).stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach((options) -> runSQL(finalManager, options));
    }

    public static void runSQL(ClusterManager manager, SQLMigrationOptions options) {
        File sqlTemplate = new File(options.getTemplate());

        if(!sqlTemplate.exists()){
            die("Cannot find file: " + options.getTemplate());
        }

        printInitMessage(options, options.getLocation() + " using " + options.getQuery());

        String template = fileAsString(sqlTemplate);
        try(Connection connection =
                    DriverManager.getConnection(options.getLocation(), options.getUsername(), options.getPassword())) {

            SQLMigrator sqlMigrator = new SQLMigrator(options.getQuery(), template, connection);

            if(options.isNo()){
                writeToSout(sqlMigrator.migrate());
            } else {
                MigrationLoader.load(manager, options.getKeyspace(), options.getBatch(), sqlMigrator);
                printWholeCompletionMessage(options);
            }

        } catch (Throwable throwable){
            die(throwable);
        }

        initiateShutdown();
    }
}
