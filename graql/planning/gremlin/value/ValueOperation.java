/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.planning.gremlin.value;

import com.google.common.collect.Iterables;
import grakn.core.core.AttributeSerialiser;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.AttributeType;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.ArrayList;
import java.util.List;

/**
 * @param <T> compared type
 * @param <U> compared persisted type
 */
public abstract class ValueOperation<T, U> {

    private final Graql.Token.Comparator comparator;
    private final T value;

    ValueOperation(Graql.Token.Comparator comparator, T value) {
        this.comparator = comparator;
        this.value = value;
    }

    public static ValueOperation<?, ?> of(ValueProperty.Operation<?> operation) {
        if (operation instanceof ValueProperty.Operation.Assignment<?>) {
            return ValueAssignment.of((ValueProperty.Operation.Assignment<?>) operation);
        } else if (operation instanceof ValueProperty.Operation.Comparison<?>) {
            return ValueComparison.of((ValueProperty.Operation.Comparison<?>) operation);
        } else {
            throw new UnsupportedOperationException("Unsupported Value Operation: " + operation.getClass());
        }
    }

    @Override
    public String toString() {
        return comparator().toString() + value();
    }

    public Graql.Token.Comparator comparator() {
        return comparator;
    }

    public T value() {
        return value;
    }

    public boolean isValueEquality() {
        return comparator.equals(Graql.Token.Comparator.EQV) && !(this instanceof ValueComparison.Variable);
    }

    public abstract U valueSerialised();

    protected abstract P<U> predicate();

    public <S, E> GraphTraversal<S, E> apply(GraphTraversal<S, E> traversal) {
        List<GraphTraversal<?, E>> valueTraversals = new ArrayList<>();
        AttributeType.DataType<?> dataType = AttributeType.DataType.of(value().getClass());
        for (AttributeType.DataType<?> comparableDataType : dataType.comparableDataTypes()) {
            Schema.VertexProperty property = Schema.VertexProperty.ofDataType(comparableDataType);
            valueTraversals.add(__.has(property.name(), predicate()));
        }

        GraphTraversal<?, E>[] array = (GraphTraversal<?, E>[]) Iterables.toArray(valueTraversals, GraphTraversal.class);

        return traversal.union(array);
    }

    public boolean test(Object otherValue) {
        if (this.value().getClass().isInstance(otherValue)) {
            // TODO: Remove this forced casting
            AttributeType.DataType<T> dataType = (AttributeType.DataType<T>) AttributeType.DataType.of(value().getClass());
            return predicate().test((U) AttributeSerialiser.of(dataType).serialise((T) otherValue));
        } else {
            return false;
        }
    }

    private int signum() {
        switch (comparator) {
            case LT:
            case LTE:
                return -1;
            case EQV:
            case NEQV:
            case CONTAINS:
                return 0;
            case GT:
            case GTE:
                return 1;
            default:
                return 0;
        }
    }

    private boolean containsEquality() {
        switch (comparator) {
            case EQV:
            case LTE:
            case GTE:
                return true;
            default:
                return false;
        }
    }

    private boolean isCompatibleWithNEQ(ValueOperation<?, ?> other) {
        if (!this.comparator().equals(Graql.Token.Comparator.NEQV)) return false;
        if (this instanceof ValueComparison.Variable || other instanceof ValueComparison.Variable) return true;

        //checks for !=/= contradiction
        return (!this.value().equals(other.value()) ||
                this.value().equals(other.value()) && !(other.comparator().equals(Graql.Token.Comparator.EQV)));
    }

    private boolean isCompatibleWithContains(ValueOperation<?, ?> other) {
        if (other.comparator().equals(Graql.Token.Comparator.CONTAINS)) return true;
        if (!other.comparator().equals(Graql.Token.Comparator.EQV)) return false;

        return (other instanceof ValueComparison.Variable ||
                other.value() instanceof String && this.predicate().test((U) other.valueSerialised()));
    }

    private boolean isCompatibleWithRegex(ValueOperation<?, ?> other) {
        if (!other.comparator().equals(Graql.Token.Comparator.EQV)) return false;

        return (other instanceof ValueComparison.Variable ||
                this.predicate().test((U) other.valueSerialised()));
    }

    //checks if the intersection of this and that is not empty
    public boolean isCompatible(ValueOperation<?, ?> other) {
        if (other.comparator().equals(Graql.Token.Comparator.NEQV)) {
            return other.isCompatibleWithNEQ(this);
        } else if (other.comparator().equals(Graql.Token.Comparator.CONTAINS)) {
            return other.isCompatibleWithContains(this);
        } else if (other.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return other.isCompatibleWithRegex(this);
        }

        if (this instanceof ValueComparison.Variable || other instanceof ValueComparison.Variable) return true;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!this.valueSerialised().getClass().equals(other.valueSerialised().getClass())) return false;

        ValueOperation<?, U> that = (ValueOperation<?, U>) other;
        return this.valueSerialised().equals(that.valueSerialised()) ?
                (this.signum() * that.signum() > 0 || this.containsEquality() && that.containsEquality()) :
                (this.predicate().test(that.valueSerialised()) || ((that.predicate()).test(this.valueSerialised())));
    }

    public boolean isSubsumedBy(ValueOperation<?, ?> other) {
        if (this.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return isCompatibleWithRegex(other);
        } else if (other.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return false;
        }

        if (this instanceof ValueComparison.Variable || other instanceof ValueComparison.Variable) return true;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!this.valueSerialised().getClass().equals(other.valueSerialised().getClass())) return false;

        ValueOperation<?, U> that = (ValueOperation<?, U>) other;
        return (that.predicate()).test(this.valueSerialised()) &&
                (this.valueSerialised().equals(that.valueSerialised()) ?
                        (this.isValueEquality() || this.isValueEquality() == that.isValueEquality()) :
                        (!this.predicate().test(that.valueSerialised())));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueOperation that = (ValueOperation) o;
        return (comparator().equals(that.comparator()) &&
                value().equals(that.value()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.comparator().hashCode();
        h *= 1000003;
        h ^= this.value().hashCode();
        return h;
    }
}