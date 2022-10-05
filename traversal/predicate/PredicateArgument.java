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

        abstract <T> boolean apply(ARG_VAL_OP operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value);

        <T> boolean apply(ARG_VAL_OP operator, AttributeVertex<T> vertex, ARG_VAL_TYPE value) {
            return apply(operator, vertex.valueType(), vertex.value(), value);
        }

        public abstract <T> boolean apply(ARG_VAL_OP operator, Encoding.ValueType<T> lhsType, T lhs, ARG_VAL_TYPE rhs);

        public static final Value<PredicateOperator.Equality, Boolean> BOOLEAN = new Value<>(Encoding.ValueType.BOOLEAN) {
            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                assert value.isBoolean();
                return apply(operator, vertex, value.getBoolean());
            }

            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Boolean rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.BOOLEAN, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Long> LONG = new Value<>(Encoding.ValueType.LONG) {
            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                assert value.isLong();
                return apply(operator, vertex, value.getLong());
            }

            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Long rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.LONG)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.LONG, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Double> DOUBLE = new Value<>(Encoding.ValueType.DOUBLE) {
            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                assert value.isDouble();
                return apply(operator, vertex, value.getDouble());
            }

            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Double rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.DOUBLE)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.DOUBLE, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, LocalDateTime> DATETIME = new Value<>(Encoding.ValueType.DATETIME) {
            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                assert value.isDateTime();
                return apply(operator, vertex, value.getDateTime());
            }

            @Override
            public <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, LocalDateTime rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.DATETIME)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.DATETIME, rhs));
            }
        };

        public static final Value<PredicateOperator, String> STRING = new Value<>(Encoding.ValueType.STRING) {
            @Override
            public <T> boolean apply(PredicateOperator operator, AttributeVertex<T> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert value.isString() || value.isRegex();
                if (operator.isSubString()) return operator.asSubString().apply(vertex.asString().value(), value);
                else return apply(operator, vertex, value.getString());
            }

            @Override
            public <T> boolean apply(PredicateOperator operator, Encoding.ValueType<T> lhsType, T lhs, String rhs) {
                assert operator.isEquality();
                if (!lhsType.comparableTo(Encoding.ValueType.STRING)) return false;
                return operator.asEquality().apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.STRING, rhs));
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
