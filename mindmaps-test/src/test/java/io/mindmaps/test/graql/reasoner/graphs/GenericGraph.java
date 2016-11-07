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

package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.QueryBuilderImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class GenericGraph {

    protected static MindmapsGraph mindmaps;
    private final static String filePath = "src/test/graql/";

    public static MindmapsGraph getGraph(String graqlFile) {
        mindmaps = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph(graqlFile);
        commit();

        return mindmaps;
    }

    public static MindmapsGraph getGraph(String ontologyFile, String... files) {
        mindmaps = Mindmaps.factory(Mindmaps.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        loadGraqlFile(ontologyFile);
        for( String graqlFile : files) {
            loadGraqlFile(graqlFile);
        }
        commit();

        return mindmaps;
    }

    private static void buildGraph(String graqlFile) {
        loadGraqlFile(graqlFile);
    }

    private static void loadGraqlFile(String fileName) {
        System.out.println("Loading " + fileName);
        if (fileName.isEmpty()) return;

        QueryBuilderImpl qb = Graql.withGraph(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath + fileName), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qb.parse(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    protected static void commit(){
        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }
    }


}
