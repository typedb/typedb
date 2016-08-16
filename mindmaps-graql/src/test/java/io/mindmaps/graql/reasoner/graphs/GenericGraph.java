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

package io.mindmaps.graql.reasoner.graphs;

import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.implementation.MindmapsValidationException;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.QueryParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GenericGraph {

    private static MindmapsTransaction mindmaps;
    private static String filePath = "src/test/resources/graql/";

    public static MindmapsTransaction getTransaction(String graqlFile) {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.getTransaction();
        buildGraph(graqlFile);

        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }

        return mindmaps;
    }

    public static MindmapsTransaction getTransaction(String ontologyFile, String ruleFile, String dataFile) {
            MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
            mindmaps = graph.getTransaction();
            buildGraph(ontologyFile, ruleFile, dataFile);

            try {
                mindmaps.commit();
            } catch (MindmapsValidationException e) {
                System.out.println(e.getMessage());
            }

            return mindmaps;
    }

    private static void buildGraph(String graqlFile) {
        loadGraqlFile(graqlFile);
    }

    private static void buildGraph(String ontologyFile, String ruleFile, String dataFile) {
            loadGraqlFile(ontologyFile);
            loadGraqlFile(ruleFile);
            loadGraqlFile(dataFile);
    }

    private static void loadGraqlFile(String fileName) {
        if (fileName.isEmpty()) return;

        QueryParser qp = QueryParser.create(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath + fileName), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qp.parseInsertQuery(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }


}
