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
 * Expected arguments are the JDBC driver, JDBC username, JDBC password, JDBC database name and the Mindmaps Graph URL
 * Optionally you can provide the name of the Mindmaps graph
 */
public class Main {

    static void die(String errorMsg){
        System.out.println(errorMsg);
        System.out.println("\nSyntax: SQLMigrator -driver <jdbc driver> -database <database url> -user <username> -pass <password> -engine <Mindmaps engine URL> [-graph <graph name>]");
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
            else
                die("Unknown option " + args[i]);
        }

        if(jdbcDriver == null) die("Please specify the JDBC diver on the classpath using -driver option");
        if (jdbcDBUrl == null) die("Please specify the URL where the SQL db is running using -database option");
        if (jdbcUser == null) die("Please specify the username of the database using the -user option");
        if (jdbcPass == null) die("Please specify the password of the database using the -pass option");
        if (engineURL == null) die("Please specify the URL where engine is running using the -engine option");

        if(graphName == null){
            graphName = jdbcDBUrl.replaceAll("^[a-zA-Z]", "_");
        }

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

            System.out.println("Data migration successful");

        }
        catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

        System.exit(0);
    }

}
