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

package io.mindmaps.graql.internal.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.EdgeLabel.AKO;

public class Traversals {

    private Traversals() {}

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Vertex, Vertex> outAkos(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.out(AKO.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Vertex, Vertex> inAkos(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.in(AKO.getLabel())).emit()).unfold();
    }
}
