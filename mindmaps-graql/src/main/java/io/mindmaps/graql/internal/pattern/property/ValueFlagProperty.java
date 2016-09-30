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

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.ConceptProperty.*;

public class ValueFlagProperty implements UniqueVarProperty, SingleTraversalProperty {

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("value");
    }

    @Override
    public GraphTraversal<Vertex, Vertex> applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.or(
                __.has(VALUE_STRING.name()),
                __.has(VALUE_LONG.name()),
                __.has(VALUE_DOUBLE.name()),
                __.has(VALUE_BOOLEAN.name())
        );
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }
}
