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

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static ai.grakn.concept.ResourceType.DataType.SUPPORTED_TYPES;

abstract class ComparatorPredicate implements ValuePredicateAdmin {

    private final Optional<Object> value;
    private final Optional<VarAdmin> var;

    private static final String[] VALUE_PROPERTIES =
            SUPPORTED_TYPES.values().stream()
                    .map(ResourceType.DataType::getConceptProperty)
                    .distinct()
                    .map(Enum::name)
                    .toArray(String[]::new);

    /**
     * @param value the value that this predicate is testing against
     */
    ComparatorPredicate(Object value) {
        if (value instanceof VarAdmin) {
            this.value = Optional.empty();
            this.var = Optional.of((VarAdmin) value);
        } else {
            // Convert integers to longs for consistency
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }

            this.value = Optional.of(value);
            this.var = Optional.empty();
        }
    }

    /**
     * @param var the variable that this predicate is testing against
     */
    ComparatorPredicate(Var var) {
        this.value = Optional.empty();
        this.var = Optional.of(var.admin());
    }

    protected abstract String getSymbol();

    abstract <V> P<V> gremlinPredicate(V value);

    public String toString() {
        // If there is no value, then there must be a var
        //noinspection OptionalGetWithoutIsPresent
        String argument = value.map(StringConverter::valueToString).orElseGet(() -> var.get().getPrintableName());

        return getSymbol() + " " + argument;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComparatorPredicate that = (ComparatorPredicate) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public Optional<P<Object>> getPredicate() {
        return value.map(this::gremlinPredicate);
    }

    @Override
    public Optional<VarAdmin> getInnerVar() {
        return var;
    }

    @Override
    public final void applyPredicate(GraphTraversal<Vertex, Vertex> traversal) {
        var.ifPresent(theVar -> {
            // Compare to another variable
            String thisVar = UUID.randomUUID().toString();
            VarName otherVar = theVar.getVarName();
            String otherValue = UUID.randomUUID().toString();

            Traversal[] traversals = Stream.of(VALUE_PROPERTIES)
                    .map(prop -> __.values(prop).as(otherValue).select(thisVar).values(prop).where(gremlinPredicate(otherValue)))
                    .toArray(Traversal[]::new);

            traversal.as(thisVar).select(otherVar.getValue()).or(traversals).select(thisVar);
        });

        value.ifPresent(theValue -> {
            // Compare to a given value
            ResourceType.DataType<?> dataType = SUPPORTED_TYPES.get(theValue.getClass().getTypeName());
            Schema.ConceptProperty property = dataType.getConceptProperty();
            traversal.has(property.name(), gremlinPredicate(theValue));
        });
    }

}
