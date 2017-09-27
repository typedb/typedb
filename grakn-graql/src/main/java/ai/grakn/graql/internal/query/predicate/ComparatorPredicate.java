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

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.util.Schema;
import ai.grakn.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static ai.grakn.concept.AttributeType.DataType.SUPPORTED_TYPES;

abstract class ComparatorPredicate implements ValuePredicate {

    private final Optional<Object> originalValue;
    private final Optional<Object> value;
    private final Optional<VarPatternAdmin> var;

    private static final String[] VALUE_PROPERTIES =
            SUPPORTED_TYPES.values().stream()
                    .map(AttributeType.DataType::getVertexProperty)
                    .distinct()
                    .map(Enum::name)
                    .toArray(String[]::new);

    /**
     * @param value the value that this predicate is testing against
     */
    ComparatorPredicate(Object value) {
        if (value instanceof VarPattern) {
            this.originalValue = Optional.empty();
            this.value = Optional.empty();
            this.var = Optional.of(((VarPattern) value).admin());
        } else {
            // Convert integers to longs for consistency
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }

            this.originalValue = Optional.of(value);

            // Convert values to how they are stored in the graph
            AttributeType.DataType dataType = AttributeType.DataType.SUPPORTED_TYPES.get(value.getClass().getName());

            if (dataType == null) {
                throw GraqlQueryException.invalidValueClass(value);
            }

            // We can trust the `SUPPORTED_TYPES` map to store things with the right type
            //noinspection unchecked
            value = dataType.getPersistenceValue(value);

            this.value = Optional.of(value);
            this.var = Optional.empty();
        }
    }

    /**
     * @param var the variable that this predicate is testing against
     */
    ComparatorPredicate(VarPattern var) {
        this.originalValue = Optional.empty();
        this.value = Optional.empty();
        this.var = Optional.of(var.admin());
    }

    protected abstract String getSymbol();

    abstract <V> P<V> gremlinPredicate(V value);

    public String toString() {
        // If there is no value, then there must be a var
        //noinspection OptionalGetWithoutIsPresent
        String argument = value.map(StringUtil::valueToString).orElseGet(() -> var.get().getPrintableName());

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
    public boolean isCompatibleWith(ValuePredicate predicate) {
        if (!(predicate instanceof EqPredicate)) return false;
        EqPredicate p = (EqPredicate) predicate;
        Object v = value.orElse(null);
        Object pval = p.equalsValue().orElse(null);
        return v == null
                || pval == null
                || gremlinPredicate(v).test(pval);
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
    public Optional<VarPatternAdmin> getInnerVar() {
        return var;
    }

    @Override
    public final <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal) {
        var.ifPresent(theVar -> {
            // Compare to another variable
            String thisVar = UUID.randomUUID().toString();
            Var otherVar = theVar.var();
            String otherValue = UUID.randomUUID().toString();

            Traversal[] traversals = Stream.of(VALUE_PROPERTIES)
                    .map(prop -> __.values(prop).as(otherValue).select(thisVar).values(prop).where(gremlinPredicate(otherValue)))
                    .toArray(Traversal[]::new);

            traversal.as(thisVar).select(otherVar.name()).or(traversals).select(thisVar);
        });

        value.ifPresent(theValue -> {
            // Compare to a given value
            AttributeType.DataType<?> dataType = SUPPORTED_TYPES.get(originalValue.get().getClass().getTypeName());
            Schema.VertexProperty property = dataType.getVertexProperty();
            traversal.has(property.name(), gremlinPredicate(theValue));
        });

        return traversal;
    }

}
