package io.mindmaps.migration.csv;

import com.google.common.collect.Lists;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.engine.loader.BlockingLoader;
import io.mindmaps.engine.loader.DistributedLoader;
import io.mindmaps.engine.loader.Loader;
import io.mindmaps.factory.MindmapsClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Main program to migrate CSV files into a Mindmaps graph. For use from a command line.
 * Expected arguments are the CSV file and the Mindmaps engine URL.
 * Additionally, name of CSV entity and name of Mindmaps graph can be provided.
 */
public class Main {

    static void die(String errorMsg) {
        System.out.println(errorMsg);
        System.out.println("\nSyntax: CSVMigrator -file <csv filename> [-graph <graph name>] [-engine <Mindmaps engine URL>] [-as <Mindmaps graph name>]");
        System.exit(-1);
    }

    public static void main(String[] args){

        String csvFileName = null;
        String csvEntityType = null;
        String engineURL = null;
        String graphName = null;

        for (int i = 0; i < args.length; i++) {
            if ("-file".equals(args[i]))
                csvFileName = args[++i];
            else if ("-graph".equals(args[i]))
                graphName = args[++i];
            else if ("-engine".equals(args[i]))
                engineURL = args[++i];
            else if ("-as".equals(args[i])){
                csvEntityType = args[++i];
            }
            else
                die("Unknown option " + args[i]);
        }

        if(csvFileName == null){
            die("Please specify CSV file using the -csv option");
        }
        File csvFile = new File(csvFileName);
        if(!csvFile.exists()){
            die("Cannot find file: " + csvFileName);
        }
        if(engineURL == null){
            die("Please specify the URL where engine is running using -engine option");
        }
        if(graphName == null){
            graphName = csvFile.getName().replaceAll("^[a-zA-Z]", "_");
        }
        if(csvEntityType == null){
            csvEntityType = csvFile.getName().replaceAll("^[a-zA-Z]", "_");
        }

        System.out.println("Migrating " + csvFileName + " using MM Engine " +
                (engineURL == null ? "local" : engineURL ) + " into graph " + graphName);


        // perform migration
        CSVSchemaMigrator schemaMigrator = new CSVSchemaMigrator();
        CSVDataMigrator dataMigrator = new CSVDataMigrator();

        //
        try{
            MindmapsGraph graph = engineURL == null ? MindmapsClient.getGraph(graphName)
                                                    : MindmapsClient.getGraph(graphName, engineURL);

            Loader loader = engineURL == null ? new BlockingLoader(graphName)
                                              : new DistributedLoader(graphName, Lists.newArrayList(engineURL));

            CSVParser csvParser = CSVParser.parse(csvFile.toURI().toURL(),
                    StandardCharsets.UTF_8, CSVFormat.DEFAULT.withHeader());

            schemaMigrator
                    .graph(graph)
                    .configure(csvEntityType, csvParser)
                    .migrate(loader);

            System.out.println("Schema migration successful");

            dataMigrator
                    .graph(graph)
                    .configure(csvEntityType, csvParser)
                    .migrate(loader);

            System.out.println("Data migration successful");

        }
        catch (Throwable throwable){
            throwable.printStackTrace(System.err);
        }

        System.exit(0);
    }
}
