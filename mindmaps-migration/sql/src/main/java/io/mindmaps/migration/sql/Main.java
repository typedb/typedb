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

import com.google.common.collect.Lists;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.factory.MindmapsClient;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Main program to migrate a SQL database into a Mindmaps graph. For use from a command line.
 * Expected arguments are the JDBC driver, JDBC username, JDBC password, JDBC database name and the Mindmaps Graph name
 * Optionally you can provide the Mindmaps engine URL
 */
public class Main {

    static void die(String errorMsg){
        System.out.println(errorMsg);
        System.out.println("\nSyntax: ./migration.sh sql -driver <jdbc driver> -database <database url> -user <username> -pass <password> [-engine <Mindmaps engine URL>] -graph <graph name>");
        System.exit(-1);
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
            else if ("-password".equals(args[i]))
                jdbcPass = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if("sql".equals(args[0]))
                continue;
            else
                die("Unknown option " + args[i]);
        }

        if (jdbcDriver == null) die("Please specify the JDBC diver on the classpath using -driver option");
        if (jdbcDBUrl == null) die("Please specify the URL where the SQL db is running using -database option");
        if (jdbcUser == null) die("Please specify the username of the database using the -user option");
        if (jdbcPass == null) die("Please specify the password of the database using the -pass option");
        if (graphName == null){ die("Please specify the name of the graph using the -graph option"); }

        System.out.println("Migrating " + jdbcDBUrl + " using MM Engine " +
                (engineURL == null ? "local" : engineURL ) + " into graph " + graphName);

        // perform migration
        SQLSchemaMigrator schemaMigrator = new SQLSchemaMigrator();
        SQLDataMigrator dataMigrator = new SQLDataMigrator();

        try{

            MindmapsGraph graph = engineURL == null ? MindmapsClient.getGraph(graphName)
                                                    : MindmapsClient.getGraph(graphName, engineURL);

            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            // make JDBC connection
            Class.forName(jdbcDriver).newInstance();
            Connection connection = DriverManager.getConnection(jdbcDBUrl, jdbcUser, jdbcPass);

            schemaMigrator
                    .graph(graph)
                    .configure(connection)
                    .migrate(loader)
                    .close();

            System.out.println("Schema migration successful");

            dataMigrator
                    .graph(graph)
                    .configure(connection)
                    .migrate(loader)
                    .close();

            System.out.println("DataType migration successful");

        }
        catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

        System.exit(0);
    }

}
