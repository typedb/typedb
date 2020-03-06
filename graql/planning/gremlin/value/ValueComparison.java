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

import grakn.core.core.AttributeSerialiser;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.AttributeType;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public abstract class ValueComparison<T, U> extends ValueOperation<T, U> {

    private final Map<Graql.Token.Comparator, Function<U, P<U>>> PREDICATES_COMPARABLE = comparablePredicates();

    ValueComparison(Graql.Token.Comparator comparator, T value) {
        super(comparator, value);
    }

    public static ValueComparison<?, ?> of(ValueProperty.Operation.Comparison<?> comparison) {
        Graql.Token.Comparator comparator = comparison.comparator();

        if (comparison instanceof ValueProperty.Operation.Comparison.Number<?>) {
            return new ValueComparison.Number<>(comparator, ((ValueProperty.Operation.Comparison.Number<?>) comparison).value());

        } else if (comparison instanceof ValueProperty.Operation.Comparison.Boolean) {
            return new ValueComparison.Boolean(comparator, ((ValueProperty.Operation.Comparison.Boolean) comparison).value());

        } else if (comparison instanceof ValueProperty.Operation.Comparison.String) {
            return new ValueComparison.String(comparator, ((ValueProperty.Operation.Comparison.String) comparison).value());

        } else if (comparison instanceof ValueProperty.Operation.Comparison.DateTime) {
            return new ValueComparison.DateTime(comparator, ((ValueProperty.Operation.Comparison.DateTime) comparison).value());

        } else if (comparison instanceof ValueProperty.Operation.Comparison.Variable) {
            return new ValueComparison.Variable(comparator, ((ValueProperty.Operation.Comparison.Variable) comparison).value());

        } else {
            throw new UnsupportedOperationException("Unsupported Value Comparison: " + comparison.getClass());
        }
    }

    private static <V> Map<Graql.Token.Comparator, Function<V, P<V>>> comparablePredicates() {
        Map<Graql.Token.Comparator, Function<V, P<V>>> predicates = new HashMap<>();
        predicates.put(Graql.Token.Comparator.EQV, P::eq);
        predicates.put(Graql.Token.Comparator.NEQV, P::neq);
        predicates.put(Graql.Token.Comparator.GT, P::gt);
        predicates.put(Graql.Token.Comparator.GTE, P::gte);
        predicates.put(Graql.Token.Comparator.LT, P::lt);
        predicates.put(Graql.Token.Comparator.LTE, P::lte);

        return Collections.unmodifiableMap(predicates);
    }

    private static Function<java.lang.String, P<java.lang.String>> containsPredicate() {
        return v -> new P<>(ValueComparison::containsIgnoreCase, v);
    }

    private static Function<java.lang.String, P<java.lang.String>> regexPredicate() {
        return v -> new P<>((value, regex) -> Pattern.matches(regex, value), v);
    }

    private static boolean containsIgnoreCase(java.lang.String str1, java.lang.String str2) {
        final int len2 = str2.length();
        if (len2 == 0) return true; // Empty string is contained

        final char first2L = Character.toLowerCase(str2.charAt(0));
        final char first2U = Character.toUpperCase(str2.charAt(0));

        for (int i = 0; i <= str1.length() - len2; i++) {
            // Quick check before calling the more expensive regionMatches() method:
            final char first1 = str1.charAt(i);
            if (first1 != first2L && first1 != first2U) continue;
            if (str1.regionMatches(true, i, str2, 0, len2)) return true;
        }
        return false;
    }

    @Override
    protected P<U> predicate() {
        Function<U, P<U>> predicate = PREDICATES_COMPARABLE.get(comparator());

        if (predicate != null) {
            return predicate.apply(valueSerialised());
        } else {
            throw new UnsupportedOperationException("Unsupported Value Comparison: " + comparator() + " on " + value().getClass());
        }
    }

    static class Number<N extends java.lang.Number> extends ValueComparison<N, N> {

        Number(Graql.Token.Comparator comparator, N value) {
            super(comparator, value);
        }

        @Override
        public N valueSerialised() {
            return new AttributeSerialiser.Default<N>().serialise(value());
        }
    }

    static class Boolean extends ValueComparison<java.lang.Boolean, java.lang.Boolean> {

        Boolean(Graql.Token.Comparator comparator, boolean value) {
            super(comparator, value);
        }

        @Override
        public java.lang.Boolean valueSerialised() {
            return AttributeSerialiser.BOOLEAN.serialise(value());
        }
    }

    static class DateTime extends ValueComparison<LocalDateTime, Long> {

        DateTime(Graql.Token.Comparator comparator, LocalDateTime value) {
            super(comparator, value);
        }

        @Override
        public Long valueSerialised() {
            return AttributeSerialiser.DATE.serialise(value());
        }
    }

    public static class String extends ValueComparison<java.lang.String, java.lang.String> {

        final Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> PREDICATES_STRING = stringPredicates();

        public String(Graql.Token.Comparator comparator, java.lang.String value) {
            super(comparator, value);
        }

        @Override
        public java.lang.String valueSerialised() {
            return AttributeSerialiser.STRING.serialise(value());
        }

        private static Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> stringPredicates() {
            Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> predicates = new HashMap<>();

            predicates.putAll(comparablePredicates());
            predicates.put(Graql.Token.Comparator.CONTAINS, containsPredicate());
            predicates.put(Graql.Token.Comparator.LIKE, regexPredicate());

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

    public static class Variable extends ValueComparison<Statement, java.lang.String> {

        private final java.lang.String gremlinVariable;

        private static final Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> PREDICATES_VAR = varPredicates();
        private static final java.lang.String[] VALUE_PROPERTIES = AttributeType.DataType.values().stream()
                .map(Schema.VertexProperty::ofDataType).distinct()
                .map(Enum::name).toArray(java.lang.String[]::new);

        Variable(Graql.Token.Comparator comparator, Statement value) {
            super(comparator, value);
            gremlinVariable = UUID.randomUUID().toString();
        }

        private static Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> varPredicates() {
            Map<Graql.Token.Comparator, Function<java.lang.String, P<java.lang.String>>> predicates = new HashMap<>();

            predicates.putAll(comparablePredicates());
            predicates.put(Graql.Token.Comparator.CONTAINS, containsPredicate());

            return Collections.unmodifiableMap(predicates);
        }

        @Override
        public  java.lang.String valueSerialised() {
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
            graql.lang.statement.Variable graqlVariable = value().var();
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

