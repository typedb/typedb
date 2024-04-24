/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.predicate;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
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

    PredicateArgument(String symbol) {
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

        Value(Encoding.ValueType<ARG_VAL_TYPE> valueType) {
            super(valueType.name());
            this.valueType = valueType;
        }

        Encoding.ValueType<ARG_VAL_TYPE> valueType() {
            return valueType;
        }

        abstract <T> boolean apply(ARG_VAL_OP operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value);

        <T> boolean apply(ARG_VAL_OP operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, ARG_VAL_TYPE value) {
            return apply(operator, vertex.valueType(), vertex.value(), value);
        }

        abstract <T> boolean apply(ARG_VAL_OP operator, Encoding.ValueType<T> lhsType, T lhs, ARG_VAL_TYPE rhs);

        public static final Value<PredicateOperator.Equality, Boolean> BOOLEAN = new Value<>(Encoding.ValueType.BOOLEAN) {
            @Override
            <T> boolean apply(PredicateOperator.Equality operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value) {
                assert value.isBoolean();
                return apply(operator, vertex, value.asBoolean().value());
            }

            @Override
            <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Boolean rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.BOOLEAN, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Long> LONG = new Value<>(Encoding.ValueType.LONG) {
            @Override
            <T> boolean apply(PredicateOperator.Equality operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value) {
                assert value.isLong();
                return apply(operator, vertex, value.asLong().value());
            }

            @Override
            <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Long rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.LONG)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.LONG, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, Double> DOUBLE = new Value<>(Encoding.ValueType.DOUBLE) {
            @Override
            <T> boolean apply(PredicateOperator.Equality operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value) {
                assert value.isDouble();
                return apply(operator, vertex, value.asDouble().value());
            }

            @Override
            <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, Double rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.DOUBLE)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.DOUBLE, rhs));
            }
        };

        public static final Value<PredicateOperator.Equality, LocalDateTime> DATETIME = new Value<>(Encoding.ValueType.DATETIME) {
            @Override
            <T> boolean apply(PredicateOperator.Equality operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value) {
                assert value.isDateTime();
                return apply(operator, vertex, value.asDateTime().value());
            }

            @Override
            <T> boolean apply(PredicateOperator.Equality operator, Encoding.ValueType<T> lhsType, T lhs, LocalDateTime rhs) {
                if (!lhsType.comparableTo(Encoding.ValueType.DATETIME)) return false;
                return operator.apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.DATETIME, rhs));
            }
        };

        public static final Value<PredicateOperator, String> STRING = new Value<>(Encoding.ValueType.STRING) {
            @Override
            <T> boolean apply(PredicateOperator operator, com.vaticle.typedb.core.graph.vertex.Value<T> vertex, Traversal.Parameters.Value<?> value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert value.isString() || value.isRegex();
                if (operator.isSubString()) return operator.asSubString().apply(vertex.asString().value(), value);
                else return apply(operator, vertex, value.asString().value());
            }

            @Override
            <T> boolean apply(PredicateOperator operator, Encoding.ValueType<T> lhsType, T lhs, String rhs) {
                assert operator.isEquality();
                if (!lhsType.comparableTo(Encoding.ValueType.STRING)) return false;
                return operator.asEquality().apply(Encoding.ValueType.compare(lhsType, lhs, Encoding.ValueType.STRING, rhs));
            }
        };
    }

    public static class Variable extends PredicateArgument {

        public static final Variable VARIABLE = new Variable();

        private Variable() {
            super("var");
        }

        boolean apply(PredicateOperator.Equality operator, com.vaticle.typedb.core.graph.vertex.Value<?> from, com.vaticle.typedb.core.graph.vertex.Value<?> to) {
            if (!from.valueType().comparableTo(to.valueType())) return false;
            Encoding.ValueType<?> valueType = to.valueType();
            if (valueType == BOOLEAN) return PredicateArgument.Value.BOOLEAN.apply(operator, from, to.asBoolean().value());
            else if (valueType == LONG) return PredicateArgument.Value.LONG.apply(operator, from, to.asLong().value());
            else if (valueType == DOUBLE) return PredicateArgument.Value.DOUBLE.apply(operator, from, to.asDouble().value());
            else if (valueType == STRING) return PredicateArgument.Value.STRING.apply(operator, from, to.asString().value());
            else if (valueType == DATETIME) return PredicateArgument.Value.DATETIME.apply(operator, from, to.asDateTime().value());
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }
}
