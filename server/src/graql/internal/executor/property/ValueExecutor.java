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

package grakn.core.graql.internal.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import graql.util.Token;
import grakn.core.graql.query.property.ValueProperty;
import grakn.core.graql.query.property.VarProperty;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ValueExecutor implements PropertyExecutor.Insertable {

    private final Variable var;
    private final ValueProperty property;
    private final ValueExecutor.Operation<?, ?> operation;

    ValueExecutor(Variable var, ValueProperty property) {
        this.var = var;
        this.property = property;
        this.operation = Operation.of(property.operation());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(EquivalentFragmentSets.value(property, var, operation));
    }

    @Override
    public Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        return ValuePredicate.create(var, property.operation(), parent);
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertValue());
    }

    class InsertValue implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            return ImmutableSet.of();
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }

        @Override
        public void execute(WriteExecutor executor) {
            if (!(operation instanceof Operation.Assignment)) {
                throw GraqlQueryException.insertPredicate();
            } else {
                executor.getBuilder(var).value(operation.value());
            }
        }
    }

    /**
     * @param <T> compared type
     * @param <U> compared persisted type
     */
    public static abstract class Operation<T, U> {

        private final Token.Comparator comparator;
        private final T value;

        Operation(Token.Comparator comparator, T value) {
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

        public Token.Comparator comparator() {
            return comparator;
        }

        public T value() {
            return value;
        }

        public boolean isValueEquality() {
            return comparator.equals(Token.Comparator.EQV) && !(this instanceof Comparison.Variable);
        }

        protected U persistedValue() {
            AttributeType.DataType dataType = AttributeType.DataType.SUPPORTED_TYPES.get(value().getClass().getName());
            return (U) dataType.getPersistedValue(value());
            // TODO: this should be a safe cast once we fix the return type of
            //       AttributeType.DataType.getPersistedValue with generics
        }

        protected abstract P<U> predicate();

        public <S, E> GraphTraversal<S, E> apply(GraphTraversal<S, E> traversal) {
            // Compare to a given value
            AttributeType.DataType<?> dataType = AttributeType.DataType.SUPPORTED_TYPES.get(value().getClass().getTypeName());
            Schema.VertexProperty property = dataType.getVertexProperty();
            traversal.has(property.name(), predicate());
            return traversal;
        }

        public boolean test(Object otherValue) {
            if (this.value().getClass().isInstance(otherValue)) {
                AttributeType.DataType dataType = AttributeType.DataType.SUPPORTED_TYPES.get(value().getClass().getName());
                return predicate().test((U) dataType.getPersistedValue(otherValue));
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

        private boolean isCompatibleWithNEQ(ValueExecutor.Operation<?, ?> other) {
            if (!this.comparator().equals(Token.Comparator.NEQV)) return false;
            if (this instanceof Comparison.Variable || other instanceof Comparison.Variable) return true;

            //checks for !=/= contradiction
            return (!this.value().equals(other.value()) ||
                    this.value().equals(other.value()) && !(other.comparator().equals(Token.Comparator.EQV)));
        }

        private boolean isCompatibleWithContains(ValueExecutor.Operation<?, ?> other) {
            if (other.comparator().equals(Token.Comparator.CONTAINS)) return true;
            if (!other.comparator().equals(Token.Comparator.EQV)) return false;

            return (other instanceof Comparison.Variable ||
                    other.value() instanceof String && this.predicate().test((U) other.persistedValue()));
        }

        private boolean isCompatibleWithRegex(ValueExecutor.Operation<?, ?> other) {
            if (!other.comparator().equals(Token.Comparator.EQV)) return false;

            return (other instanceof Comparison.Variable ||
                    this.predicate().test((U) other.persistedValue()));
        }

        public boolean isCompatible(ValueExecutor.Operation<?, ?> other) {
            if (other.comparator().equals(Token.Comparator.NEQV)) {
                return other.isCompatibleWithNEQ(this);
            } else if (other.comparator().equals(Token.Comparator.CONTAINS)) {
                return other.isCompatibleWithContains(this);
            } else if (other.comparator().equals(Token.Comparator.LIKE)) {
                return other.isCompatibleWithRegex(this);
            }

            if (this instanceof Comparison.Variable || other instanceof Comparison.Variable) return true;
            //NB this is potentially dangerous e.g. if a user types a long as a char in the query
            if (!this.persistedValue().getClass().equals(other.persistedValue().getClass())) return false;

            ValueExecutor.Operation<?, U> that = (ValueExecutor.Operation<?, U>) other;
            return this.persistedValue().equals(that.persistedValue()) ?
                    (this.signum() * that.signum() > 0 || this.containsEquality() && that.containsEquality()) :
                    (this.predicate().test(that.persistedValue()) || ((that.predicate()).test(this.persistedValue())));
        }

        public boolean subsumes(ValueExecutor.Operation<?, ?> other) {
            if (this.comparator().equals(Token.Comparator.LIKE)) {
                return isCompatibleWithRegex(other);
            } else if (other.comparator().equals(Token.Comparator.LIKE)) {
                return false;
            }

            if (this instanceof Comparison.Variable || other instanceof Comparison.Variable) return true;
            //NB this is potentially dangerous e.g. if a user types a long as a char in the query
            if (!this.persistedValue().getClass().equals(other.persistedValue().getClass())) return false;

            ValueExecutor.Operation<?, U> that = (ValueExecutor.Operation<?, U>) other;
            return (that.predicate()).test(this.persistedValue()) &&
                    (this.persistedValue().equals(that.persistedValue()) ?
                            (this.isValueEquality() || this.isValueEquality() == that.isValueEquality()) :
                            (!this.predicate().test(that.persistedValue())));
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

        static abstract class Assignment<T, U> extends Operation<T, U> {

            Assignment(T value) {
                super(Token.Comparator.EQV, value);
            }

            static Assignment<?, ?> of(ValueProperty.Operation.Assignment<?> assignment) {
                if (assignment instanceof ValueProperty.Operation.Assignment.Number<?>) {
                    return new Number<>(((ValueProperty.Operation.Assignment.Number<?>) assignment).value());

                } else if (assignment instanceof ValueProperty.Operation.Assignment.Boolean) {
                    return new Assignment.Boolean(((ValueProperty.Operation.Assignment.Boolean) assignment).value());

                } else if (assignment instanceof ValueProperty.Operation.Assignment.String) {
                    return new Assignment.String(((ValueProperty.Operation.Assignment.String) assignment).value());

                } else if (assignment instanceof ValueProperty.Operation.Assignment.DateTime) {
                    return new Assignment.DateTime(((ValueProperty.Operation.Assignment.DateTime) assignment).value());

                } else {
                    throw new UnsupportedOperationException("Unsupported Value Assignment: " + assignment.getClass());
                }
            }

            @Override
            protected P<U> predicate() {
                return P.eq(persistedValue());
            }

            static class Number<N extends java.lang.Number> extends Assignment<N, java.lang.Number> {

                Number(N value) {
                    super(value);
                }
            }

            static class Boolean extends Assignment<java.lang.Boolean, java.lang.Boolean> {

                Boolean(boolean value) {
                    super(value);
                }
            }

            static class DateTime extends Assignment<LocalDateTime, Long> {

                DateTime(LocalDateTime value) {
                    super(value);
                }
            }

            static class String extends Assignment<java.lang.String, java.lang.String> {

                String(java.lang.String value) {
                    super(value);
                }
            }
        }

        public static abstract class Comparison<T, U> extends Operation<T, U> {

            final Map<Token.Comparator, Function<U, P<U>>> PREDICATES_COMPARABLE = comparablePredicates();

            Comparison(Token.Comparator comparator, T value) {
                super(comparator, value);
            }

            static Comparison<?, ?> of(ValueProperty.Operation.Comparison<?> comparison) {
                Token.Comparator comparator = comparison.comparator();

                if (comparison instanceof ValueProperty.Operation.Comparison.Number<?>) {
                    return new Comparison.Number<>(comparator, ((ValueProperty.Operation.Comparison.Number<?>) comparison).value());

                } else if (comparison instanceof ValueProperty.Operation.Comparison.Boolean) {
                    return new Comparison.Boolean(comparator, ((ValueProperty.Operation.Comparison.Boolean) comparison).value());

                } else if (comparison instanceof ValueProperty.Operation.Comparison.String) {
                    return new Comparison.String(comparator, ((ValueProperty.Operation.Comparison.String) comparison).value());

                } else if (comparison instanceof ValueProperty.Operation.Comparison.DateTime) {
                    return new Comparison.DateTime(comparator, ((ValueProperty.Operation.Comparison.DateTime) comparison).value());

                } else if (comparison instanceof ValueProperty.Operation.Comparison.Variable) {
                    return new Comparison.Variable(comparator, ((ValueProperty.Operation.Comparison.Variable) comparison).value());

                } else {
                    throw new UnsupportedOperationException("Unsupported Value Comparison: " + comparison.getClass());
                }
            }

            static <V> Map<Token.Comparator, Function<V, P<V>>> comparablePredicates() {
                Map<Token.Comparator, Function<V, P<V>>> predicates = new HashMap<>();
                predicates.put(Token.Comparator.EQV, P::eq);
                predicates.put(Token.Comparator.NEQV, P::neq);
                predicates.put(Token.Comparator.GT, P::gt);
                predicates.put(Token.Comparator.GTE, P::gte);
                predicates.put(Token.Comparator.LT, P::lt);
                predicates.put(Token.Comparator.LTE, P::lte);

                return Collections.unmodifiableMap(predicates);
            }

            static Function<java.lang.String, P<java.lang.String>> containsPredicate() {
                return v -> new P<>(java.lang.String::contains, v);
            }

            static Function<java.lang.String, P<java.lang.String>> regexPredicate() {
                return v -> new P<>((value, regex) -> Pattern.matches(regex, value), v);
            }

            @Override
            protected P<U> predicate() {
                Function<U, P<U>> predicate = PREDICATES_COMPARABLE.get(comparator());

                if (predicate != null) {
                    return predicate.apply(persistedValue());
                } else {
                    throw new UnsupportedOperationException("Unsupported Value Comparison: " + comparator() + " on " + value().getClass());
                }
            }

            static class Number<N extends java.lang.Number> extends Comparison<N, N> {

                Number(Token.Comparator comparator, N value) {
                    super(comparator, value);
                }
            }

            static class Boolean extends Comparison<java.lang.Boolean, java.lang.Boolean> {

                Boolean(Token.Comparator comparator, boolean value) {
                    super(comparator, value);
                }
            }

            static class DateTime extends Comparison<LocalDateTime, Long> {

                DateTime(Token.Comparator comparator, LocalDateTime value) {
                    super(comparator, value);
                }
            }

            public static class String extends Comparison<java.lang.String, java.lang.String> {

                final Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> PREDICATES_STRING = stringPredicates();

                public String(Token.Comparator comparator, java.lang.String value) {
                    super(comparator, value);
                }

                private static Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> stringPredicates() {
                    Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> predicates = new HashMap<>();

                    predicates.putAll(comparablePredicates());
                    predicates.put(Token.Comparator.CONTAINS, containsPredicate());
                    predicates.put(Token.Comparator.LIKE, regexPredicate());

                    return Collections.unmodifiableMap(predicates);
                }

                @Override
                protected P<java.lang.String> predicate() {
                    Function<java.lang.String, P<java.lang.String>> predicate = PREDICATES_STRING.get(comparator());

                    if (predicate != null) {
                        return predicate.apply(value());
                    } else {
                        throw new UnsupportedOperationException("Unsupported Value Comparison: " + comparator() + " on " + value().getClass());
                    }
                }

            }

            public static class Variable extends Comparison<Statement, java.lang.String> {

                private final java.lang.String gremlinVariable;

                private static final Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> PREDICATES_VAR = varPredicates();
                private static final java.lang.String[] VALUE_PROPERTIES = AttributeType.DataType.SUPPORTED_TYPES.values().stream()
                        .map(dataType -> dataType.getVertexProperty()).distinct()
                        .map(vertexProperty -> vertexProperty.name()).toArray(java.lang.String[]::new);

                Variable(Token.Comparator comparator, Statement value) {
                    super(comparator, value);
                    gremlinVariable = UUID.randomUUID().toString();
                }

                private static Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> varPredicates() {
                    Map<Token.Comparator, Function<java.lang.String, P<java.lang.String>>> predicates = new HashMap<>();

                    predicates.putAll(comparablePredicates());
                    predicates.put(Token.Comparator.CONTAINS, containsPredicate());

                    return Collections.unmodifiableMap(predicates);
                }

                @Override
                protected java.lang.String persistedValue() {
                    return null;
                }

                @Override
                protected P<java.lang.String> predicate() {
                    Function<java.lang.String, P<java.lang.String>> predicate = PREDICATES_VAR.get(comparator());

                    if (predicate != null) {
                        return predicate.apply(gremlinVariable);
                    } else {
                        throw new UnsupportedOperationException("Unsupported Variable Comparison: " + comparator());
                    }
                }

                @Override
                public <S, E> GraphTraversal<S, E> apply(GraphTraversal<S, E> traversal) {
                    // Compare to another variable
                    grakn.core.graql.query.statement.Variable graqlVariable = value().var();
                    java.lang.String gremlinVariable2 = UUID.randomUUID().toString();

                    traversal.as(gremlinVariable2).select(graqlVariable.symbol()).or(
                            Stream.of(VALUE_PROPERTIES)
                                    .map(prop -> __.values(prop).as(gremlinVariable).select(gremlinVariable2).values(prop).where(predicate()))
                                    .toArray(Traversal[]::new)
                    ).select(gremlinVariable2);
                    return traversal;
                }
            }
        }
    }
}
