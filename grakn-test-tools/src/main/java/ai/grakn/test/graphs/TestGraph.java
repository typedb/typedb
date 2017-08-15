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

package ai.grakn.test.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;

import java.util.Set;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;

/**
 * Base for all test graphs.
 * @author borislav
 *
 */
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

    public static Thing putEntity(GraknGraph graph, String id, EntityType type, Label key) {
        Thing inst = type.addEntity();
        putResource(inst, graph.getSchemaConcept(key), id);
        return inst;
    }

    public static <T> void putResource(Thing thing, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = resourceType.putResource(resource);
        thing.resource(resourceInstance);
    }

    public static Thing getInstance(GraknGraph graph, String id){
        Set<Thing> things = graph.getResourcesByValue(id)
                .stream().flatMap(Resource::ownerInstances).collect(toSet());
        if (things.size() != 1) {
            throw new IllegalStateException("Multiple things with given resource value");
        }
        return things.iterator().next();
    }
}
