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
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
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
        private final Map<Pair<Identifier.Variable, Predicate.Value<?, ?>>, Set<Value>> values;
        private final Map<Identifier.Variable, Pair<Predicate.Value<?, ?>, Value>> largestGTPredicates;
        private final Map<Identifier.Variable, Pair<Predicate.Value<?, ?>, Value>> smallestLTPredicates;

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

        public void pushValue(Identifier.Variable identifier, Predicate.Value<?, ?> predicate, Value value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new HashSet<>()).add(value);

            if (predicate.operator() == GT || predicate.operator() == GTE) {
                Pair<Predicate.Value<?, ?>, Value> previous = largestGTPredicates.get(identifier);
                if (previous == null || previous.second().compareTo(value) < 0) {
                    largestGTPredicates.put(identifier, new Pair<>(predicate, value));
                }
            } else if (predicate.operator() == LT || predicate.operator() == LTE) {
                Pair<Predicate.Value<?, ?>, Value> previous = smallestLTPredicates.get(identifier);
                if (previous == null || previous.second().compareTo(value) > 0) {
                    smallestLTPredicates.put(identifier, new Pair<>(predicate, value));
                }
            }
        }

        public VertexIID.Thing getIID(Identifier.Variable identifier) {
            return iids.get(identifier);
        }

        public Set<Value> getValues(Identifier.Variable identifier, Predicate.Value<?, ?> predicate) {
            return values.get(pair(identifier, predicate));
        }

        public Optional<Pair<Predicate.Value<?, ?>, Value>> largestGTValue(Identifier.Variable id) {
            return Optional.ofNullable(largestGTPredicates.get(id));
        }

        public Optional<Pair<Predicate.Value<?, ?>, Value>> smallestLTValue(Identifier.Variable id) {
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

        // TODO: we could specialise this into a subtype per value and then parametrise it properly, improving comparison, value retrieval etc.
        public static class Value implements Comparable<Value> {

            private final Encoding.ValueType<?> valueType;
            private final Boolean booleanVal;
            private final Long longVal;
            private final Double doubleVal;
            private final String stringVal;
            private final LocalDateTime dateTimeVal;
            private final Pattern regexPattern;
            private final int hash;

            Value(boolean value) {
                this(BOOLEAN, value, null, null, null, null, null);
            }

            Value(long value) {
                this(LONG, null, value, null, null, null, null);
            }

            Value(double value) {
                this(DOUBLE, null, null, value, null, null, null);
            }

            Value(LocalDateTime value) {
                this(DATETIME, null, null, null, value, null, null);
            }

            Value(String value) {
                this(STRING, null, null, null, null, value, null);
            }

            Value(Pattern regex) {
                this(STRING, null, null, null, null, null, regex);
            }

            private Value(Encoding.ValueType<?> valueType, Boolean booleanVal, Long longVal, Double doubleVal,
                          LocalDateTime dateTimeVal, String stringVal, Pattern regexPattern) {
                this.valueType = valueType;
                this.booleanVal = booleanVal;
                this.longVal = longVal;
                this.doubleVal = doubleVal;
                this.dateTimeVal = dateTimeVal;
                this.stringVal = stringVal;
                this.regexPattern = regexPattern;
                this.hash = Objects.hash(valueType, booleanVal, longVal, doubleVal, dateTimeVal, stringVal, regexPattern);
            }

            public Encoding.ValueType<?> valueType() {
                return valueType;
            }

            public boolean isBoolean() {
                return booleanVal != null;
            }

            public boolean isLong() {
                return longVal != null;
            }

            public boolean isDouble() {
                return doubleVal != null;
            }

            public boolean isDateTime() {
                return dateTimeVal != null;
            }

            public boolean isString() {
                return stringVal != null;
            }

            public boolean isRegex() {
                return regexPattern != null;
            }

            public Boolean getBoolean() {
                return booleanVal;
            }

            public Long getLong() {
                return longVal;
            }

            public Double getDouble() {
                if (isDouble()) return doubleVal;
                else if (isLong()) return longVal.doubleValue();
                else return null;
            }

            public LocalDateTime getDateTime() {
                return dateTimeVal;
            }

            public String getString() {
                return stringVal;
            }

            public Pattern getRegex() {
                return regexPattern;
            }

            @Override
            public String toString() {
                if (isBoolean()) return "boolean: " + booleanVal;
                else if (isLong()) return "long: " + longVal;
                else if (isDouble()) return "double: " + doubleVal;
                else if (isDateTime()) return "datetime: " + dateTimeVal;
                else if (isString()) return "string: " + stringVal;
                else if (isRegex()) return "regex: " + regexPattern.pattern();
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Value that = (Value) o;
                return (Objects.equals(this.valueType, that.valueType) &&
                        Objects.equals(this.booleanVal, that.booleanVal) &&
                        Objects.equals(this.longVal, that.longVal) &&
                        Objects.equals(this.doubleVal, that.doubleVal) &&
                        Objects.equals(this.dateTimeVal, that.dateTimeVal) &&
                        Objects.equals(this.stringVal, that.stringVal) &&
                        Objects.equals(this.regexPattern, that.regexPattern));
            }

            @Override
            public int hashCode() {
                return hash;
            }

            public Object value() {
                if (isBoolean()) return booleanVal;
                else if (isLong()) return longVal;
                else if (isDouble()) return doubleVal;
                else if (isString()) return stringVal;
                else if (isDateTime()) return dateTimeVal;
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public int compareTo(Value other) {
                if (!valueType.comparableTo(other.valueType)) {
                    throw TypeDBException.of(VALUES_NOT_COMPARABLE, valueType, other.valueType);
                }
                //TODO: this is ugly as hell
                return Encoding.ValueType.compare((Encoding.ValueType<Object>) valueType, value(),
                        (Encoding.ValueType<Object>) other.valueType, other.value());
            }
        }
    }
}
