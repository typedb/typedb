package io.mindmaps.reasoner.graphs;


import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class WineGraph {
    private static MindmapsTransaction mindmaps;

    public static MindmapsTransaction getTransaction() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.newTransaction();
        buildGraph();

        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }

        return mindmaps;
    }

    private static void buildGraph() {
        addOntology();
        addInstances();
        addRules();
    }

    private static void addOntology(){

        QueryParser qp = QueryParser.create(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/resources/graql/wines-ontology.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qp.parseInsertQuery(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }
    private static void addInstances() {

        QueryParser qp = QueryParser.create(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/resources/graql/wines-data.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qp.parseInsertQuery(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    private static void addRules()
    {

        QueryParser qp = QueryParser.create(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/resources/graql/wines-rules.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qp.parseInsertQuery(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

}
