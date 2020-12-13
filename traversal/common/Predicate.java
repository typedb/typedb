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

package grakn.core.traversal.common;

import grakn.core.common.exception.GraknException;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.traversal.Traversal;
import graql.lang.common.GraqlToken;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.graph.util.Encoding.ValueType.DOUBLE_PRECISION;

public abstract class Predicate<PRED_OP extends Predicate.Operator, PRED_ARG extends Predicate.Argument> {

    final PRED_OP operator;
    final PRED_ARG argument;
    private final int hash;

    private Predicate(PRED_OP operator, PRED_ARG argument) {
        this.operator = operator;
        this.argument = argument;
        this.hash = Objects.hash(operator, argument);
    }

    public PRED_OP operator() {
        return operator;
    }

    @Override
    public String toString() {
        return operator + " " + argument;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Predicate<?, ?> that = (Predicate<?, ?>) o;
        return this.operator.equals(that.operator) && this.argument.equals(that.argument);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static abstract class Value<VAL_OP extends Operator> extends Predicate<VAL_OP, Argument.Value<VAL_OP, ?>> {

        public Value(VAL_OP operator, Argument.Value<VAL_OP, ?> argument) {
            super(operator, argument);
        }

        public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
            return argument.apply(operator, vertex, value);
        }

        public static class Equality extends Value<Operator.Equality> {

            public Equality(Operator.Equality operator, Argument.Value<Operator.Equality, ?> argument) {
                super(operator, argument);
            }

            public static Predicate.Value.Equality of(GraqlToken.Predicate.Equality token, Argument.Value<Operator.Equality, ?> argument) {
                return new Predicate.Value.Equality(Predicate.Operator.Equality.of(token), argument);
            }
        }

        public static class SubString extends Value<Operator> {

            public SubString(Operator operator, Argument.Value<Operator, String> argument) {
                super(operator, argument);
            }

            public static Predicate.Value.SubString of(GraqlToken.Predicate token) {
                return new Predicate.Value.SubString(Predicate.Operator.SubString.of(token), Argument.Value.STRING);
            }
        }
    }

    public static class Variable extends Predicate<Operator.Equality, Argument.Variable> {


        private Variable(Operator.Equality operator) {
            super(operator, Argument.Variable.VARIABLE);
        }

        public static Predicate.Variable of(GraqlToken.Predicate.Equality token) {
            return new Predicate.Variable(Operator.Equality.of(token));
        }

        public boolean apply(AttributeVertex<?> fromVertex, AttributeVertex<?> toVertex) {
            return argument.apply(operator, fromVertex, toVertex);
        }

        public Predicate.Variable reflection() {
            return new Predicate.Variable(operator.reflection());
        }
    }

    public static abstract class Operator {

        private final GraqlToken.Predicate token;

        protected Operator(GraqlToken.Predicate token) {
            this.token = token;
        }

        public static Operator of(GraqlToken.Predicate token) {
            if (token.isEquality()) return Equality.of(token.asEquality());
            else if (token.isSubString()) return SubString.of(token.asSubString());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        boolean isEquality() { return false; }

        boolean isSubString() { return false; }

        Operator.Equality asEquality() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Equality.class));
        }

        Operator.SubString asSubString() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(SubString.class));
        }

        @Override
        public String toString() {
            return token.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Operator that = (Operator) o;
            return this.token.equals(that.token);
        }

        @Override
        public int hashCode() {
            return token.hashCode();
        }

        public static abstract class Equality extends Operator {

            public Equality(GraqlToken.Predicate.Equality token) {
                super(token);
            }

            abstract boolean apply(int comparisonResult);

            abstract Equality reflection();

            @Override
            boolean isEquality() { return true; }

            @Override
            Operator.Equality asEquality() { return this; }

            public static final Equality EQ = new Equality(GraqlToken.Predicate.Equality.EQ) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult == 0; }

                @Override
                Equality reflection() { return this; }
            };

            public static final Equality NEQ = new Equality(GraqlToken.Predicate.Equality.NEQ) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult != 0; }

                @Override
                Equality reflection() { return this; }
            };

            public static final Equality GT = new Equality(GraqlToken.Predicate.Equality.GT) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult > 0; }

                @Override
                Equality reflection() { return LT; }
            };

            public static final Equality GTE = new Equality(GraqlToken.Predicate.Equality.GTE) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult >= 0; }

                @Override
                Equality reflection() { return LTE; }
            };

            public static final Equality LT = new Equality(GraqlToken.Predicate.Equality.LT) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult < 0; }

                @Override
                Equality reflection() { return GT; }
            };

            public static final Equality LTE = new Equality(GraqlToken.Predicate.Equality.LTE) {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult <= 0; }

                @Override
                Equality reflection() { return GTE; }
            };

            private static final Map<GraqlToken.Predicate.Equality, Equality> operators = map(
                    pair(GraqlToken.Predicate.Equality.EQ, Equality.EQ),
                    pair(GraqlToken.Predicate.Equality.NEQ, Equality.NEQ),
                    pair(GraqlToken.Predicate.Equality.GT, Equality.GT),
                    pair(GraqlToken.Predicate.Equality.GTE, Equality.GTE),
                    pair(GraqlToken.Predicate.Equality.LT, Equality.LT),
                    pair(GraqlToken.Predicate.Equality.LTE, Equality.LTE)
            );

            public static Operator.Equality of(GraqlToken.Predicate.Equality operator) {
                return Equality.operators.get(operator);
            }
        }

        public static abstract class SubString extends Operator {

            public SubString(GraqlToken.Predicate.SubString token) {
                super(token);
            }

            abstract boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue);

            @Override
            boolean isSubString() { return true; }

            @Override
            Operator.SubString asSubString() { return this; }

            private static final SubString CONTAINS = new SubString(GraqlToken.Predicate.SubString.CONTAINS) {
                @Override
                boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue) {
                    assert predicateValue.isString();
                    return vertexValue.contains(predicateValue.getString());
                }
            };

            private static final SubString LIKE = new SubString(GraqlToken.Predicate.SubString.LIKE) {
                @Override
                boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue) {
                    assert predicateValue.isRegex();
                    return predicateValue.getRegex().matcher(vertexValue).matches();
                }
            };

            private static final Map<GraqlToken.Predicate.SubString, SubString> operators = map(
                    pair(GraqlToken.Predicate.SubString.CONTAINS, SubString.CONTAINS),
                    pair(GraqlToken.Predicate.SubString.LIKE, SubString.LIKE)
            );

            private static Operator.SubString of(GraqlToken.Predicate.SubString token) {
                return operators.get(token);
            }
        }
    }

    public abstract static class Argument {

        private final String name;

        protected Argument(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "<" + name + ">";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Argument that = (Argument) o;
            return this.name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        public static abstract class Value<ARG_VAL_OP extends Operator, ARG_VAL_TYPE> extends Argument {

            public Value(String name) {
                super(name);
            }

            public abstract boolean apply(ARG_VAL_OP operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value);

            public abstract boolean apply(ARG_VAL_OP operator, AttributeVertex<?> vertex, ARG_VAL_TYPE value);

            private static int compareDoubles(double first, double second) {
                int res = java.lang.Double.compare(first, second);
                if (res == 0) return 0;
                else if (Math.abs(first - second) < DOUBLE_PRECISION) return 0;
                else return res;
            }

            public static Value<Operator.Equality, Boolean> BOOLEAN = new Value<Operator.Equality, Boolean>("boolean") {
                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                    assert value.isBoolean();
                    return apply(operator, vertex, value.getBoolean());
                }

                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Boolean value) {
                    if (!vertex.valueType().comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                    assert vertex.isBoolean();
                    return vertex.asBoolean().value() == value;
                }
            };

            public static Value<Operator.Equality, Long> LONG = new Value<Operator.Equality, Long>("long") {
                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                    assert value.isLong();
                    return apply(operator, vertex, value.getLong());
                }

                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Long value) {
                    if (!vertex.valueType().comparableTo(Encoding.ValueType.LONG)) return false;
                    assert (vertex.isLong() || vertex.isDouble());

                    if (vertex.isLong()) return operator.apply(vertex.asLong().value().compareTo(value));
                    else if (vertex.isDouble())
                        return operator.apply(Value.compareDoubles(vertex.asDouble().value(), value));
                    else throw GraknException.of(ILLEGAL_STATE);
                }
            };

            public static Value<Operator.Equality, Double> DOUBLE = new Value<Operator.Equality, Double>("double") {
                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                    assert value.isDouble();
                    return apply(operator, vertex, value.getDouble());
                }

                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Double value) {
                    if (!vertex.valueType().comparableTo(Encoding.ValueType.DOUBLE)) return false;
                    assert (vertex.isLong() || vertex.isDouble());

                    double vertexValue;
                    if (vertex.isLong()) vertexValue = vertex.asLong().value();
                    else if (vertex.isDouble()) vertexValue = vertex.asDouble().value();
                    else throw GraknException.of(ILLEGAL_STATE);
                    return operator.apply(Value.compareDoubles(vertexValue, value));
                }
            };

            public static Value<Operator.Equality, LocalDateTime> DATETIME = new Value<Operator.Equality, LocalDateTime>("datetime") {
                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                    assert value.isDateTime();
                    return apply(operator, vertex, value.getDateTime());
                }

                @Override
                public boolean apply(Operator.Equality operator, AttributeVertex<?> vertex, LocalDateTime value) {
                    if (!vertex.valueType().comparableTo(Encoding.ValueType.DATETIME)) return false;
                    assert vertex.isDateTime();

                    return operator.apply(vertex.asDateTime().value().compareTo(value));
                }
            };

            public static Value<Operator, String> STRING = new Value<Operator, String>("string") {
                @Override
                public boolean apply(Operator operator, AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                    assert value.isString() || value.isRegex();
                    if (operator.isSubString()) return operator.asSubString().apply(vertex.asString().value(), value);
                    else return apply(operator, vertex, value.getString());
                }

                @Override
                public boolean apply(Operator operator, AttributeVertex<?> vertex, String value) {
                    if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                    assert vertex.isString() && operator.isEquality();
                    return operator.asEquality().apply(vertex.asString().value().compareTo(value));
                }
            };
        }

        public static class Variable extends Argument {

            public static Variable VARIABLE = new Variable();

            public Variable() {
                super("var");
            }

            public boolean apply(Operator.Equality operator, AttributeVertex<?> from, AttributeVertex<?> to) {
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
                        throw GraknException.of(ILLEGAL_STATE);
                }
            }
        }
    }
}
