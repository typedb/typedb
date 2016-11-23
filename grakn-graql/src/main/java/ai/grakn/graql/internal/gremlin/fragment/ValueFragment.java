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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.concept.ResourceType;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.concept.ResourceType.DataType.SUPPORTED_TYPES;

class ValueFragment extends AbstractFragment {

    private final ValuePredicateAdmin predicate;

    ValueFragment(String start, ValuePredicateAdmin predicate) {
        super(start);
        this.predicate = predicate;
    }

    @Override
    public void applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        Object value = predicate.getPredicate().getValue();

        if (value != null) {
            // Look up on a single key (e.g. VALUE_STRING)
            ResourceType.DataType<?> dataType = SUPPORTED_TYPES.get(value.getClass().getTypeName());
            Schema.ConceptProperty property = dataType.getConceptProperty();
            traversal.has(property.name(), predicate.getPredicate());
        } else {
            // Look up on all keys if necessary (not indexable)
            Traversal[] hasTraversals = SUPPORTED_TYPES.values().stream().map(dataType -> {
                Schema.ConceptProperty property = dataType.getConceptProperty();
                return __.has(property.name(), predicate.getPredicate());
            }).toArray(Traversal[]::new);
            traversal.or(hasTraversals);
        }
    }

    @Override
    public String getName() {
        return "[value:" + predicate + "]";
    }

    @Override
    public long fragmentCost(long previousCost) {
        if (predicate.isSpecific()) {
            return NUM_RESOURCES_PER_VALUE;
        } else {
            return previousCost;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ValueFragment that = (ValueFragment) o;

        return predicate != null ? predicate.equals(that.predicate) : that.predicate == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
        return result;
    }
}
