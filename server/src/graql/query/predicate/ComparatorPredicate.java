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

package grakn.core.graql.query.predicate;

import grakn.core.graql.concept.AttributeType.DataType;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Abstract class for value predicates involving comparators.
 */
public abstract class ComparatorPredicate implements ValuePredicate {

    // Exactly one of these fields will be present
    private final Object value;

    private static final String[] VALUE_PROPERTIES = DataType.SUPPORTED_TYPES.values().stream()
            .map(dataType -> dataType.getVertexProperty()).distinct()
            .map(vertexProperty -> vertexProperty.name()).toArray(name -> new String[name]);

    /**
     * @param value the value that this predicate is testing against
     */
    ComparatorPredicate(Object value) {
        if (value == null) {
            throw new NullPointerException("Value is null");

        } else if (value instanceof Integer) {
            this.value = ((Integer) value).longValue();

        } else {
            this.value = value;
        }
    }

    protected abstract String getSymbol();

    abstract <V> P<V> gremlinPredicate(V value);

    final Optional<Object> persistedValue() {
        if (value instanceof Statement) {
            return Optional.empty();
        } else {
            // Convert values to how they are stored in the graph
            DataType dataType = DataType.SUPPORTED_TYPES.get(value.getClass().getName());

            if (dataType == null) {
                throw GraqlQueryException.invalidValueClass(value);
            }

            // We can trust the `SUPPORTED_TYPES` map to store things with the right type
            //noinspection unchecked
            return Optional.of(dataType.getPersistedValue(value));
        }
    }

    public final Optional<Object> value() {
        if (value instanceof Statement) {
            return Optional.empty();
        } else {
            return Optional.of(value);
        }
    }

    public String toString() {
        if (value instanceof Statement) {
            return getSymbol() + Query.Char.SPACE + ((Statement) value).getPrintableName();
        } else {
            return getSymbol() + Query.Char.SPACE + StringUtil.valueToString(value);
        }
    }

    /**
     * @return returns the sign of inequality, +ve for going in increasing, -ve for decreasing direction and 0 for 0
     */
    public abstract int signum();

    /**
     * @return true if the inequality contains equality
     */
    public boolean containsEquality(){ return false;}

    @Override @SuppressWarnings("Duplicates")
    public boolean isCompatibleWith(ValuePredicate predicate) {
        if (!(predicate instanceof ComparatorPredicate)) {
            return predicate.isCompatibleWith(this);

        } else if (predicate instanceof ContainsPredicate || predicate instanceof NeqPredicate ){
            return predicate.isCompatibleWith(this);

        }

        ComparatorPredicate that = (ComparatorPredicate) predicate;
        Object val = this.value().orElse(null);
        Object thatVal = that.value().orElse(null);
        if (val == null || thatVal == null) return true;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!val.getClass().equals(thatVal.getClass())) return false;

        return val.equals(thatVal) ?
                (this.signum() * that.signum() > 0 || this.containsEquality() && that.containsEquality()) :
                (this.gremlinPredicate(val).test(thatVal) || that.gremlinPredicate(thatVal).test(val));
    }

    @Override @SuppressWarnings("Duplicates")
    public boolean subsumes(ValuePredicate predicate) {
        if (!(predicate instanceof ComparatorPredicate)) {
            return false;
        }

        ComparatorPredicate that = (ComparatorPredicate) predicate;
        Object val = this.value().orElse(null);
        Object thatVal = that.value().orElse(null);
        if (val == null || thatVal == null) return true;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!val.getClass().equals(thatVal.getClass())) return false;

        return that.gremlinPredicate(thatVal).test(val) &&
                (val.equals(thatVal) ?
                        (this.isSpecific() || this.isSpecific() == that.isSpecific()) :
                        (!this.gremlinPredicate(val).test(thatVal)));
    }

    @Override
    public Optional<P<Object>> getPredicate() {
        return persistedValue().map(value -> gremlinPredicate(value));
    }

    @Override
    public Optional<Statement> getInnerVar() {
        if (value instanceof Statement) {
            return Optional.of((Statement) value);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public final <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal) {
        if (value instanceof Statement) {
            // Compare to another variable
            String thisVar = UUID.randomUUID().toString();
            Variable otherVar = ((Statement) value).var();
            String otherValue = UUID.randomUUID().toString();

            Traversal[] traversals = Stream.of(VALUE_PROPERTIES)
                    .map(prop -> __.values(prop).as(otherValue).select(thisVar).values(prop).where(gremlinPredicate(otherValue)))
                    .toArray(Traversal[]::new);

            traversal.as(thisVar).select(otherVar.symbol()).or(traversals).select(thisVar);
            return traversal;

        } else {
            // Compare to a given value
            DataType<?> dataType = DataType.SUPPORTED_TYPES.get(value().get().getClass().getTypeName());
            Schema.VertexProperty property = dataType.getVertexProperty();
            traversal.has(property.name(), gremlinPredicate(value));
            return traversal;

        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComparatorPredicate that = (ComparatorPredicate) o;
        return persistedValue().equals(that.persistedValue());
    }

    @Override
    public int hashCode() {
        return persistedValue().hashCode();
    }
}