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

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ValueProperty implements NamedProperty, SingleTraversalProperty {

    private final ValuePredicateAdmin predicate;

    public ValueProperty(ValuePredicateAdmin predicate) {
        this.predicate = predicate;
    }

    public ValuePredicateAdmin getPredicate() {
        return predicate;
    }

    @Override
    public String getName() {
        return "value";
    }

    @Override
    public String getProperty() {
        return predicate.toString();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        Schema.ConceptProperty value = getValuePropertyForPredicate(predicate);
        return traversal.has(value.name(), predicate.getPredicate());
    }

    @Override
    public FragmentPriority getPriority() {
        if (predicate.isSpecific()) {
            return FragmentPriority.VALUE_SPECIFIC;
        } else {
            return FragmentPriority.VALUE_NONSPECIFIC;
        }
    }

    /**
     * @param predicate a predicate to test on a vertex
     * @return the correct VALUE property to check on the vertex for the given predicate
     */
    private Schema.ConceptProperty getValuePropertyForPredicate(ValuePredicateAdmin predicate) {
        Object value = predicate.getInnerValues().iterator().next();
        return ResourceType.DataType.SUPPORTED_TYPES.get(value.getClass().getTypeName()).getConceptProperty();
    }
}
