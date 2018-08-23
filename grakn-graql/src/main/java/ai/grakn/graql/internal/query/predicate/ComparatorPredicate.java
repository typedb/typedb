/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.concept.AttributeType.DataType;
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

abstract class ComparatorPredicate implements ValuePredicate {

    // Exactly one of these fields will be present
    private final Optional<Object> value;
    private final Optional<VarPatternAdmin> var;

    private static final String[] VALUE_PROPERTIES =
            DataType.SUPPORTED_TYPES.values().stream()
                    .map(DataType::getVertexProperty)
                    .distinct()
                    .map(Enum::name)
                    .toArray(String[]::new);

    /**
     * @param value the value that this predicate is testing against
     */
    ComparatorPredicate(Object value) {
        if (value instanceof VarPattern) {
            this.value = Optional.empty();
            this.var = Optional.of(((VarPattern) value).admin());
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
    ComparatorPredicate(VarPattern var) {
        this.value = Optional.empty();
        this.var = Optional.of(var.admin());
    }

    protected abstract String getSymbol();

    abstract <V> P<V> gremlinPredicate(V value);

    final Optional<Object> persistedValue() {
        return value().map(value -> {

            // Convert values to how they are stored in the graph
            DataType dataType = DataType.SUPPORTED_TYPES.get(value.getClass().getName());

            if (dataType == null) {
                throw GraqlQueryException.invalidValueClass(value);
            }

            // We can trust the `SUPPORTED_TYPES` map to store things with the right type
            //noinspection unchecked
            return dataType.getPersistenceValue(value);
        });
    }

    final public Optional<Object> value() {
        return value;
    }

    public String toString() {
        // If there is no value, then there must be a var
        //noinspection OptionalGetWithoutIsPresent
        String argument = persistedValue().map(StringUtil::valueToString).orElseGet(() -> var.get().getPrintableName());

        return getSymbol() + " " + argument;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComparatorPredicate that = (ComparatorPredicate) o;
        return persistedValue().equals(that.persistedValue());
    }
    @Override
    public boolean isCompatibleWith(ValuePredicate predicate) {
        ComparatorPredicate that = (ComparatorPredicate) predicate;
        Object val = this.value().orElse(null);
        Object thatVal = that.value().orElse(null);
        if (val == null || thatVal == null) return true;

        //checks for !=/= contradiction
        return ((!val.equals(thatVal))
                || ((!(this instanceof EqPredicate) || !(that instanceof NeqPredicate))
                && (!(that instanceof NeqPredicate) || !(this instanceof EqPredicate))))
                && (this.gremlinPredicate(val).test(thatVal)
                || that.gremlinPredicate(thatVal).test(val));
    }

    @Override
    public int hashCode() {
        return persistedValue().hashCode();
    }

    @Override
    public Optional<P<Object>> getPredicate() {
        return persistedValue().map(this::gremlinPredicate);
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

        persistedValue().ifPresent(theValue -> {
            // Compare to a given value
            DataType<?> dataType = DataType.SUPPORTED_TYPES.get(value().get().getClass().getTypeName());
            Schema.VertexProperty property = dataType.getVertexProperty();
            traversal.has(property.name(), gremlinPredicate(theValue));
        });

        return traversal;
    }

}
