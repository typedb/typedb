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

package grakn.core.graql.executor.property.value;

import com.google.common.collect.Iterables;
import grakn.core.concept.type.AttributeType;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.Serialiser;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

/**
 * @param <T> compared type
 * @param <U> compared persisted type
 */
public abstract class Operation<T, U> {

    private final Graql.Token.Comparator comparator;
    private final T value;

    Operation(Graql.Token.Comparator comparator, T value) {
        this.comparator = comparator;
        this.value = value;
    }

    public static Operation<?, ?> of(ValueProperty.Operation<?> operation) {
        if (operation instanceof ValueProperty.Operation.Assignment<?>) {
            return Assignment.of((ValueProperty.Operation.Assignment<?>) operation);
        } else if (operation instanceof ValueProperty.Operation.Comparison<?>) {
            return Comparison.of((ValueProperty.Operation.Comparison<?>) operation);
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
        return comparator.equals(Graql.Token.Comparator.EQV) && !(this instanceof Comparison.Variable);
    }

    abstract U valueSerialised();

    protected abstract P<U> predicate();

    protected abstract Set<AttributeType.DataType<?>> comparableDataTypes();

    public <S, E> GraphTraversal<S, E> apply(GraphTraversal<S, E> traversal) {
        List<GraphTraversal<?, E>> valueTraversals = new ArrayList<>();
        for (AttributeType.DataType<?> dataType : comparableDataTypes()) {
            Schema.VertexProperty property = Schema.VertexProperty.ofDataType(dataType);
            valueTraversals.add(__.has(property.name(), predicate()));
        }

        GraphTraversal<?, E>[] array = (GraphTraversal<?, E>[]) Iterables.toArray(valueTraversals, GraphTraversal.class);

        return traversal.union(array);
    }

    public boolean test(Object otherValue) {
        if (this.value().getClass().isInstance(otherValue)) {
            // TODO: Remove this forced casting
            AttributeType.DataType<T> dataType =
                    (AttributeType.DataType<T>) AttributeType.DataType.of(value().getClass());
            return predicate().test((U) Serialiser.of(dataType).serialise((T) otherValue));
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

    private boolean isCompatibleWithNEQ(Operation<?, ?> other) {
        if (!this.comparator().equals(Graql.Token.Comparator.NEQV)) return false;
        if (this instanceof Comparison.Variable || other instanceof Comparison.Variable) return true;

        //checks for !=/= contradiction
        return (!this.value().equals(other.value()) ||
                this.value().equals(other.value()) && !(other.comparator().equals(Graql.Token.Comparator.EQV)));
    }

    private boolean isCompatibleWithContains(Operation<?, ?> other) {
        if (other.comparator().equals(Graql.Token.Comparator.CONTAINS)) return true;
        if (!other.comparator().equals(Graql.Token.Comparator.EQV)) return false;

        return (other instanceof Comparison.Variable ||
                other.value() instanceof String && this.predicate().test((U) other.valueSerialised()));
    }

    private boolean isCompatibleWithRegex(Operation<?, ?> other) {
        if (!other.comparator().equals(Graql.Token.Comparator.EQV)) return false;

        return (other instanceof Comparison.Variable ||
                this.predicate().test((U) other.valueSerialised()));
    }

    //checks if the intersection of this and that is not empty
    public boolean isCompatible(Operation<?, ?> other) {
        if (other.comparator().equals(Graql.Token.Comparator.NEQV)) {
            return other.isCompatibleWithNEQ(this);
        } else if (other.comparator().equals(Graql.Token.Comparator.CONTAINS)) {
            return other.isCompatibleWithContains(this);
        } else if (other.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return other.isCompatibleWithRegex(this);
        }

        if (this instanceof Comparison.Variable || other instanceof Comparison.Variable) return true;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!this.valueSerialised().getClass().equals(other.valueSerialised().getClass())) return false;

        Operation<?, U> that = (Operation<?, U>) other;
        return this.valueSerialised().equals(that.valueSerialised()) ?
                (this.signum() * that.signum() > 0 || this.containsEquality() && that.containsEquality()) :
                (this.predicate().test(that.valueSerialised()) || ((that.predicate()).test(this.valueSerialised())));
    }

    //for subsumption to take place, this needs to define a subset of that
    public boolean subsumes(Operation<?, ?> other) {
        if (this.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return this.isCompatibleWithRegex(other);
        } else if (other.comparator().equals(Graql.Token.Comparator.LIKE)) {
            return false;
        }

        Operation<?, U> that = (Operation<?, U>) other;
        if (this instanceof Comparison.Variable || that instanceof Comparison.Variable) return false;
        //NB this is potentially dangerous e.g. if a user types a long as a char in the query
        if (!this.valueSerialised().getClass().equals(that.valueSerialised().getClass())) return false;
        if (this.valueSerialised().getClass() == String.class) {
            return (that.predicate()).test(this.valueSerialised()) &&
                    (this.valueSerialised().equals(that.valueSerialised()) ?
                            (this.isValueEquality() || this.isValueEquality() == that.isValueEquality()) :
                            (!this.predicate().test(that.valueSerialised())));
        }

        double thatValue = Double.parseDouble(that.valueSerialised().toString());
        double thatLeftBound, thatRightBound;
        boolean thatHardLeftBound, thatHardRightBound;
        int thatSignum = that.signum();
        if (thatSignum < 0) {
            thatLeftBound = Double.NEGATIVE_INFINITY;
            thatRightBound = thatValue;
            thatHardLeftBound = true;
            thatHardRightBound = that.containsEquality();
        } else if (thatSignum > 0) {
            thatLeftBound = thatValue;
            thatRightBound = Double.POSITIVE_INFINITY;
            thatHardLeftBound = that.containsEquality();
            thatHardRightBound = true;
        } else {
            thatLeftBound = thatValue;
            thatRightBound = thatValue;
            thatHardLeftBound = thatHardRightBound = true;
        }

        int thisSignum = this.signum();
        double thisValue = Double.parseDouble(this.valueSerialised().toString());
        double thisLeftBound = thisSignum < 0 ? Double.NEGATIVE_INFINITY : thisValue;
        double thisRightBound = thisSignum > 0 ? Double.POSITIVE_INFINITY : thisValue;

        boolean singleNeqPresent = this.comparator().equals(Graql.Token.Comparator.NEQV)
                != that.comparator().equals(Graql.Token.Comparator.NEQV);
        boolean eqInconsistency = thisSignum == 0 && thatSignum == 0
                && this.containsEquality() != that.containsEquality()
                || singleNeqPresent;
        return !eqInconsistency
                && thisLeftBound >= thatLeftBound && thisRightBound <= thatRightBound
                && (thisLeftBound != thatLeftBound || thatHardLeftBound)
                && (thisRightBound != thatRightBound || thatHardRightBound);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Operation that = (Operation) o;
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