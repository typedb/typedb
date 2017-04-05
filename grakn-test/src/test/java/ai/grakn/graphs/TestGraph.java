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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Query;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public abstract class TestGraph {

    protected void buildOntology(GraknGraph graph){};

    protected void buildInstances(GraknGraph graph){};

    protected void buildRelations(GraknGraph graph){};

    protected void buildRules(GraknGraph graph){};

    public Consumer<GraknGraph> build() {
        return (GraknGraph graph) -> {
            buildOntology(graph);
            buildInstances(graph);
            buildRelations(graph);
            buildRules(graph);
        };
    }

    static Instance putEntity(GraknGraph graph, String id, EntityType type, TypeLabel key) {
        Instance inst = type.addEntity();
        putResource(inst, graph.getType(key), id);
        return inst;
    }

    static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = resourceType.putResource(resource);
        instance.resource(resourceInstance);
    }

    static Instance getInstance(GraknGraph graph, String id){
        Set<Instance> instances = graph.getResourcesByValue(id)
                .stream().flatMap(res -> res.ownerInstances().stream()).collect(toSet());
        if (instances.size() != 1)
            throw new IllegalStateException("Multiple instances with given resource value");
        return instances.iterator().next();
    }

    public static void loadFromFile(GraknGraph graph, String file) {
        try {
            File graql = new File("src/test/graql/" + file);

            graph.graql()
                    .parseList(Files.readLines(graql, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .forEach(Query::execute);
        } catch (IOException |GraknValidationException e){
            throw new RuntimeException(e);
        }
    }
}
