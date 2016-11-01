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

package ai.grakn.migration.sql;

import com.google.common.collect.Lists;
import ai.grakn.engine.loader.BlockingLoader;
import ai.grakn.engine.loader.DistributedLoader;
import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.util.ConfigProperties;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Main program to migrate a SQL database into a Grakn graph. For use from a command line.
 * Expected arguments are the JDBC driver, JDBC username, JDBC password, JDBC database name and the Grakn Graph name
 * Optionally you can provide the Grakn engine URL
 */
public class Main {

    private static ConfigProperties properties = ConfigProperties.getInstance();

    static void die(String errorMsg){
        throw new RuntimeException(errorMsg + "\nSyntax: ./migration.sh sql -driver <jdbc driver> -database <database url> -user <username> -pass <password> [-engine <Grakn engine URL>] [-graph <graph name>]");
    }

    public static void main(String[] args){

        String jdbcDriver = null;
        String jdbcDBUrl = null;
        String jdbcUser = null;
        String jdbcPass = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-driver".equals(args[i]))
                jdbcDriver = args[++i];
            else if ("-database".equals(args[i]))
                jdbcDBUrl = args[++i];
            else if ("-user".equals(args[i]))
                jdbcUser = args[++i];
            else if ("-pass".equals(args[i]))
                jdbcPass = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if(i == 0 && "sql".equals(args[i]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        if (jdbcDriver == null) die("Please specify the JDBC diver on the classpath using -driver option");
        if (jdbcDBUrl == null) die("Please specify the URL where the SQL db is running using -database option");
        if (jdbcUser == null) die("Please specify the username of the database using the -user option");
        if (jdbcPass == null) die("Please specify the password of the database using the -pass option");

        if (graphName == null){
            graphName = properties.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        }

        System.out.println("Migrating " + jdbcDBUrl + " using MM Engine " +
                (engineURL == null ? "local" : engineURL ) + " into graph " + graphName);

        // perform migration
        SQLSchemaMigrator schemaMigrator = new SQLSchemaMigrator();
        SQLDataMigrator dataMigrator = new SQLDataMigrator();

        try{
            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            // make JDBC connection
            Class.forName(jdbcDriver).newInstance();
            Connection connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass);

            schemaMigrator
                    .configure(connection)
                    .migrate(loader)
                    .close();

            System.out.println("Schema migration successful");

            connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass);
            dataMigrator
                    .configure(connection)
                    .migrate(loader)
                    .close();

            System.out.println("Data migration successful");

        }
        catch (Throwable throwable){
           die(throwable.getMessage());
        }
    }

}
