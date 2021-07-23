/*
 * Copyright (C) 2021 Vaticle
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
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.traversal.GraphTraversal;

import java.time.LocalDateTime;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

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

        private final Encoding.ValueType valueType;

        public Value(Encoding.ValueType valueType) {
            super(valueType.name());
            this.valueType = valueType;
        }

        public Encoding.ValueType valueType() {
            return valueType;
        }

        public abstract boolean apply(ARG_VAL_OP operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value);

        public abstract boolean apply(ARG_VAL_OP operator, AttributeVertex<?> vertex, ARG_VAL_TYPE value);

        public static Value<PredicateOperator.Equality, Boolean> BOOLEAN = new Value<PredicateOperator.Equality, Boolean>(Encoding.ValueType.BOOLEAN) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
                assert value.isBoolean();
                return apply(operator, vertex, value.getBoolean());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Boolean value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                assert vertex.isBoolean();
                return vertex.asBoolean().value().equals(value);
            }
        };

        public static Value<PredicateOperator.Equality, Long> LONG = new Value<PredicateOperator.Equality, Long>(Encoding.ValueType.LONG) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
                assert value.isLong();
                return apply(operator, vertex, value.getLong());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Long value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.LONG)) return false;
                assert (vertex.isLong() || vertex.isDouble());

                if (vertex.isLong()) return operator.apply(vertex.asLong().value().compareTo(value));
                else if (vertex.isDouble())
                    return operator.apply(Predicate.compareDoubles(vertex.asDouble().value(), value));
                else throw TypeDBException.of(ILLEGAL_STATE);
            }
        };

        public static Value<PredicateOperator.Equality, Double> DOUBLE = new Value<PredicateOperator.Equality, Double>(Encoding.ValueType.DOUBLE) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
                assert value.isDouble();
                return apply(operator, vertex, value.getDouble());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, Double value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DOUBLE)) return false;
                assert (vertex.isLong() || vertex.isDouble());

                double vertexValue;
                if (vertex.isLong()) vertexValue = vertex.asLong().value();
                else if (vertex.isDouble()) vertexValue = vertex.asDouble().value();
                else throw TypeDBException.of(ILLEGAL_STATE);
                return operator.apply(Predicate.compareDoubles(vertexValue, value));
            }
        };

        public static Value<PredicateOperator.Equality, LocalDateTime> DATETIME = new Value<PredicateOperator.Equality, LocalDateTime>(Encoding.ValueType.DATETIME) {
            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
                assert value.isDateTime();
                return apply(operator, vertex, value.getDateTime());
            }

            @Override
            public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> vertex, LocalDateTime value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DATETIME)) return false;
                assert vertex.isDateTime();

                return operator.apply(vertex.asDateTime().value().compareTo(value));
            }
        };

        public static Value<PredicateOperator, String> STRING = new Value<PredicateOperator, String>(Encoding.ValueType.STRING) {
            @Override
            public boolean apply(PredicateOperator operator, AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert value.isString() || value.isRegex();
                if (operator.isSubString()) return operator.asSubString().apply(vertex.asString().value(), value);
                else return apply(operator, vertex, value.getString());
            }

            @Override
            public boolean apply(PredicateOperator operator, AttributeVertex<?> vertex, String value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert vertex.isString() && operator.isEquality();
                return operator.asEquality().apply(vertex.asString().value().compareTo(value));
            }
        };
    }

    public static class Variable extends PredicateArgument {

        public static Variable VARIABLE = new Variable();

        public Variable() {
            super("var");
        }

        public boolean apply(PredicateOperator.Equality operator, AttributeVertex<?> from, AttributeVertex<?> to) {
            if (!from.valueType().comparableTo(to.valueType())) return false;

            switch (to.valueType()) {
                case BOOLEAN:
                    return Value.BOOLEAN.apply(operator, from, to.asBoolean().value());
                case LONG:
                    return Value.LONG.apply(operator, from, to.asLong().value());
                case DOUBLE:
                    return Value.DOUBLE.apply(operator, from, to.asDouble().value());
                case STRING:
                    return Value.STRING.apply(operator, from, to.asString().value());
                case DATETIME:
                    return Value.DATETIME.apply(operator, from, to.asDateTime().value());
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }
}
