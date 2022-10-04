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

package com.vaticle.typedb.core.traversal.predicate;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.traversal.Traversal;

import java.time.LocalDateTime;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;

public abstract class PredicateArgument {

    private final String symbol;

    protected PredicateArgument(String symbol) {
        this.symbol = symbol;
    }

    @Override
    public String toString() {
        return "<" + symbol + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PredicateArgument that = (PredicateArgument) o;
        return this.symbol.equals(that.symbol);
    }

    @Override
    public int hashCode() {
        return symbol.hashCode();
    }

    public static abstract class Value<ARG_VAL_OP extends PredicateOperator, ARG_VAL_TYPE> extends PredicateArgument {

        private final Encoding.ValueType<ARG_VAL_TYPE> valueType;

        public Value(Encoding.ValueType<ARG_VAL_TYPE> valueType) {
            super(valueType.name());
            this.valueType = valueType;
        }

        public Encoding.ValueType<ARG_VAL_TYPE> valueType() {
            return valueType;
        }

        public abstract <T> boolean apply(ARG_VAL_OP operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value);

        public abstract <T> boolean apply(ARG_VAL_OP operator, AttributeVertex<T> vertex, ARG_VAL_TYPE value);

        public abstract boolean apply(ARG_VAL_OP operator, ARG_VAL_TYPE lhs, ARG_VAL_TYPE rhs);

        public static final Value<PredicateOperator.Equality, Boolean> BOOLEAN = new Value<>(Encoding.ValueType.BOOLEAN) {
            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                assert value.isBoolean();
                return apply(operator, vertex, value.getBoolean());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Boolean value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                assert vertex.isBoolean();
                return apply(operator, vertex.asBoolean().value(), value);
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, Boolean lhs, Boolean rhs) {
                return operator.apply(valueType().comparator().compare(lhs, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Long> LONG = new Value<>(Encoding.ValueType.LONG) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                assert value.isLong();
                return apply(operator, vertex, value.getLong());
            }

            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Long value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.LONG)) return false;
                assert (vertex.isLong() || vertex.isDouble());

                Encoding.ValueType<T> tValueType = vertex.valueType();
                T val = vertex.value();
                compare(tValueType, val);

                if (vertex.isLong()) return apply(operator, vertex.asLong().value(), value);
                else if (vertex.isDouble()) {
                    return DOUBLE.apply(operator, vertex.asDouble().value(), value.doubleValue());
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }

            private <T> boolean compare(Encoding.ValueType<T> valueType, T value) {
                return false;
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, Long lhs, Long rhs) {
                return operator.apply(valueType().comparator().compare(lhs, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Double> DOUBLE = new Value<>(Encoding.ValueType.DOUBLE) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                assert value.isDouble();
                return apply(operator, vertex, value.getDouble());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Double value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DOUBLE)) return false;
                assert (vertex.isLong() || vertex.isDouble());

                if (vertex.isLong()) return apply(operator, vertex.asLong().value().doubleValue(), value);
                else if (vertex.isDouble()) return apply(operator, vertex.asDouble().value(), value);
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, Double lhs, Double rhs) {
                return operator.apply(valueType().comparator().compare(lhs, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, LocalDateTime> DATETIME = new Value<>(Encoding.ValueType.DATETIME) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                assert value.isDateTime();
                return apply(operator, vertex, value.getDateTime());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, LocalDateTime value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DATETIME)) return false;
                assert vertex.isDateTime();
                return apply(operator, vertex.asDateTime().value(), value);
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, LocalDateTime lhs, LocalDateTime rhs) {
                return operator.apply(valueType().comparator().compare(lhs, rhs));
            }
        };

        public static final Value<PredicateOperator, String> STRING = new Value<>(Encoding.ValueType.STRING) {
            @Override
            public boolean apply(PredicateOperator operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert value.isString() || value.isRegex();
                if (operator.isSubString()) return operator.asSubString().apply(vertex.asString().value(), value);
                else return apply(operator, vertex, value.getString());
            }

            @Override
            public boolean apply(PredicateOperator operator, AttributeVertex<?> vertex, String value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert vertex.isString() && operator.isEquality();
                return operator.asEquality().apply(Encoding.ValueType.STRING.comparator().compare(vertex.asString().value(), value));
            }

            @Override
            public boolean apply(PredicateOperator operator, String lhs, String rhs) {
                assert operator.isEquality();
                return operator.asEquality().apply(valueType().comparator().compare(lhs, rhs));
            }
        };
    }

    public static class Variable extends PredicateArgument {

        public static final Variable VARIABLE = new Variable();

        public Variable() {
            super("var");
        }

        public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> from, AttributeVertex<?> to) {
            if (!from.valueType().comparableTo(to.valueType())) return false;
            Encoding.ValueType<?> valueType = to.valueType();
            if (valueType == BOOLEAN) return Value.BOOLEAN.apply(operator, from, to.asBoolean().value());
            else if (valueType == LONG) return Value.LONG.apply(operator, from, to.asLong().value());
            else if (valueType == DOUBLE) return Value.DOUBLE.apply(operator, from, to.asDouble().value());
            else if (valueType == STRING) return Value.STRING.apply(operator, from, to.asString().value());
            else if (valueType == DATETIME) return Value.DATETIME.apply(operator, from, to.asDateTime().value());
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }
}
