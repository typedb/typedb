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

package io.grakn.test.graql.reasoner.graphs;

import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.exception.GraknValidationException;
import io.grakn.graql.Graql;
import io.grakn.graql.QueryBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class GenericGraph {

    protected static GraknGraph grakn;
    private final static String filePath = "src/test/graql/";

    public static GraknGraph getGraph(String graqlFile) {
        grakn = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph(graqlFile);
        commit();

        return grakn;
    }

    public static GraknGraph getGraph(String ontologyFile, String... files) {
        grakn = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        loadGraqlFile(ontologyFile);
        for( String graqlFile : files) {
            loadGraqlFile(graqlFile);
        }
        commit();

        return grakn;
    }

    private static void buildGraph(String graqlFile) {
        loadGraqlFile(graqlFile);
    }

    private static void loadGraqlFile(String fileName) {
        System.out.println("Loading " + fileName);
        if (fileName.isEmpty()) return;

        QueryBuilder qb = Graql.withGraph(grakn);
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
            grakn.commit();
        } catch (GraknValidationException e) {
            System.out.println(e.getMessage());
        }
    }


}
