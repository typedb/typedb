/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.VALUES_NOT_COMPARABLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.GT;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.GTE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.LT;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.LTE;

public abstract class Traversal {

    final Structure structure;
    final Parameters parameters;
    final Modifiers modifiers;

    Traversal() {
        structure = new Structure();
        parameters = new Parameters();
        modifiers = new Modifiers();
    }

    public Structure structure() {
        return structure;
    }

    public Parameters parameters() {
        return parameters;
    }

    public Modifiers modifiers() {
        return modifiers;
    }

    abstract FunctionalIterator<VertexMap> permutationIterator(GraphManager graphMgr);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Traversal that = (Traversal) o;
        return this.structure.equals(that.structure) && this.parameters.equals(that.parameters) && this.modifiers.equals(that.modifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.structure, this.parameters, this.modifiers);
    }

    public static class Parameters {

        private final Map<Identifier.Variable, VertexIID.Thing> iids;
        private final Map<Pair<Identifier.Variable, Predicate.Value<?, ?>>, Set<Value<?>>> values;
        private final Map<Identifier.Variable, Pair<Predicate.Value<?, ?>, Value<?>>> largestGTPredicates;
        private final Map<Identifier.Variable, Pair<Predicate.Value<?, ?>, Value<?>>> smallestLTPredicates;

        public Parameters() {
            iids = new HashMap<>();
            values = new HashMap<>();
            largestGTPredicates = new HashMap<>();
            smallestLTPredicates = new HashMap<>();
        }

        public void putIID(Identifier.Variable identifier, VertexIID.Thing iid) {
            assert !this.iids.containsKey(identifier);
            this.iids.put(identifier, iid);
        }

        public void pushValue(Identifier.Variable identifier, Predicate.Value<?, ?> predicate, Value<?> value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new HashSet<>()).add(value);

            if (predicate.operator() == GT || predicate.operator() == GTE) {
                Pair<Predicate.Value<?, ?>, Value<?>> previous = largestGTPredicates.get(identifier);
                if (previous == null || previous.second().compareTo(value) < 0) {
                    largestGTPredicates.put(identifier, new Pair<>(predicate, value));
                }
            } else if (predicate.operator() == LT || predicate.operator() == LTE) {
                Pair<Predicate.Value<?, ?>, Value<?>> previous = smallestLTPredicates.get(identifier);
                if (previous == null || previous.second().compareTo(value) > 0) {
                    smallestLTPredicates.put(identifier, new Pair<>(predicate, value));
                }
            }
        }

        public VertexIID.Thing getIID(Identifier.Variable identifier) {
            return iids.get(identifier);
        }

        public Set<Value<?>> getValues(Identifier.Variable identifier, Predicate.Value<?, ?> predicate) {
            return values.get(pair(identifier, predicate));
        }

        public Optional<Pair<Predicate.Value<?, ?>, Value<?>>> largestGTValue(Identifier.Variable id) {
            return Optional.ofNullable(largestGTPredicates.get(id));
        }

        public Optional<Pair<Predicate.Value<?, ?>, Value<?>>> smallestLTValue(Identifier.Variable id) {
            return Optional.ofNullable(smallestLTPredicates.get(id));
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder().append("Parameters: {");
            if (!iids.isEmpty()) str.append("\n\tiid: ").append(iids);
            if (!values.isEmpty()) str.append("\n\tvalues: ").append(values);
            str.append("\n}");
            return str.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Parameters that = (Parameters) o;

            return iids.equals(that.iids) && values.equals(that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(iids, values);
        }

        public static class Value<T> implements Comparable<Value<?>> {

            private final Encoding.ValueType<T> valueType;
            private final T value;
            private final int hash;

            private Value(Encoding.ValueType<T> valueType, T value) {
                this.valueType = valueType;
                this.value = value;
                this.hash = Objects.hash(valueType, value);
            }

            public Encoding.ValueType<T> valueType() {
                return valueType;
            }

            public T value() {
                return value;
            }

            public boolean isBoolean() {
                return false;
            }

            public Boolean asBoolean() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Boolean.class));
            }

            public boolean isLong() {
                return false;
            }

            public Long asLong() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Long.class));
            }

            public boolean isDouble() {
                return false;
            }

            public Double asDouble() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Double.class));
            }

            public boolean isDateTime() {
                return false;
            }

            public DateTime asDateTime() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(DateTime.class));
            }

            public boolean isString() {
                return false;
            }

            public String asString() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(String.class));
            }

            public boolean isRegex() {
                return false;
            }

            public Regex asRegex() {
                throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Regex.class));
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Value<T> that = (Value<T>) o;
                return valueType.equals(that.valueType) && value.equals(that.value);
            }

            @Override
            public int hashCode() {
                return hash;
            }

            @Override
            public int compareTo(Value<?> other) {
                if (!valueType.comparableTo(other.valueType)) {
                    throw TypeDBException.of(VALUES_NOT_COMPARABLE, valueType, other.valueType);
                }
                return compareTyped(other);
            }

            private <U> int compareTyped(Value<U> other) {
                return Encoding.ValueType.compare(valueType, value, other.valueType(), other.value());
            }


            public static class Boolean extends Value<java.lang.Boolean> {

                Boolean(java.lang.Boolean value) {
                    super(BOOLEAN, value);
                }

                @Override
                public boolean isBoolean() {
                    return true;
                }

                @Override
                public Boolean asBoolean() {
                    return this;
                }
            }

            public static class Long extends Value<java.lang.Long> {

                Long(java.lang.Long value) {
                    super(LONG, value);
                }

                @Override
                public boolean isLong() {
                    return true;
                }

                @Override
                public Long asLong() {
                    return this;
                }
            }

            public static class Double extends Value<java.lang.Double> {

                Double(java.lang.Double value) {
                    super(DOUBLE, value);
                }

                @Override
                public boolean isDouble() {
                    return true;
                }

                @Override
                public Double asDouble() {
                    return this;
                }
            }

            public static class String extends Value<java.lang.String> {

                String(java.lang.String value) {
                    super(STRING, value);
                }

                @Override
                public boolean isString() {
                    return true;
                }

                @Override
                public String asString() {
                    return this;
                }
            }

            public static class DateTime extends Value<LocalDateTime> {

                DateTime(LocalDateTime value) {
                    super(DATETIME, value);
                }

                @Override
                public boolean isDateTime() {
                    return true;
                }

                @Override
                public DateTime asDateTime() {
                    return this;
                }
            }

            public static class Regex extends Value<java.lang.String> {

                private final Pattern pattern;

                Regex(java.lang.String value) {
                    super(STRING, value);
                    this.pattern = Pattern.compile(value);
                }

                public Pattern pattern() {
                    return pattern;
                }

                @Override
                public boolean isRegex() {
                    return true;
                }

                @Override
                public Regex asRegex() {
                    return this;
                }
            }
        }
    }
}
