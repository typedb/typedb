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

import java.util.Map;
import java.util.Objects;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Predicate<OPERATOR extends Predicate.Operator> {

    protected final OPERATOR operator;
    private final int hash;

    protected Predicate(OPERATOR operator) {
        this.operator = operator;
        hash = Objects.hash(getClass(), operator);
    }

    public OPERATOR operator() {
        return operator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Predicate<?> that = (Predicate<?>) o;
        return this.operator == that.operator;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static abstract class Operator {

        public static Operator of(GraqlToken.Predicate token) {
            if (token.isEquality()) return Equality.of(token.asEquality());
            else if (token.isSubString()) return SubString.of(token.asSubString());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        boolean isEquality() { return false; }

        boolean isSubString() { return false; }

        Operator.Equality asEquality() {
            throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Equality.class)));
        }

        Operator.SubString asSubString() {
            throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(SubString.class)));
        }

        public static abstract class Equality extends Operator {

            abstract boolean apply(int comparisonResult);

            abstract Equality reflection();

            @Override
            boolean isEquality() { return true; }

            @Override
            Operator.Equality asEquality() { return this; }

            public static final Equality EQ = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult == 0; }

                @Override
                Equality reflection() { return this; }
            };

            public static final Equality NEQ = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult != 0; }

                @Override
                Equality reflection() { return this; }
            };

            public static final Equality GT = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult > 0; }

                @Override
                Equality reflection() { return LTE; }
            };

            public static final Equality GTE = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult >= 0; }

                @Override
                Equality reflection() { return LT; }
            };

            public static final Equality LT = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult < 0; }

                @Override
                Equality reflection() { return GTE; }
            };

            public static final Equality LTE = new Equality() {
                @Override
                boolean apply(int comparisonResult) { return comparisonResult <= 0; }

                @Override
                Equality reflection() { return GT; }
            };

            private static final Map<GraqlToken.Predicate.Equality, Operator.Equality> operators = map(
                    pair(GraqlToken.Predicate.Equality.EQ, Equality.EQ),
                    pair(GraqlToken.Predicate.Equality.NEQ, Equality.NEQ),
                    pair(GraqlToken.Predicate.Equality.GT, Equality.GT),
                    pair(GraqlToken.Predicate.Equality.GTE, Equality.GTE),
                    pair(GraqlToken.Predicate.Equality.LT, Equality.LT),
                    pair(GraqlToken.Predicate.Equality.LTE, Equality.LTE)
            );

            private static Operator.Equality of(GraqlToken.Predicate.Equality operator) {
                return Equality.operators.get(operator);
            }
        }

        private static abstract class SubString extends Operator {

            abstract boolean apply(java.lang.String vertexValue, Traversal.Parameters.Value predicateValue);

            @Override
            boolean isSubString() { return true; }

            @Override
            Operator.SubString asSubString() { return this; }

            private static final SubString CONTAINS = new SubString() {
                @Override
                boolean apply(java.lang.String vertexValue, Traversal.Parameters.Value predicateValue) {
                    assert predicateValue.isString();
                    return vertexValue.contains(predicateValue.getString());
                }
            };

            private static final SubString LIKE = new SubString() {
                @Override
                boolean apply(java.lang.String vertexValue, Traversal.Parameters.Value predicateValue) {
                    assert predicateValue.isRegex();
                    return predicateValue.getRegex().matcher(vertexValue).matches();
                }
            };

            private static final Map<GraqlToken.Predicate.SubString, Operator.SubString> operators = map(
                    pair(GraqlToken.Predicate.SubString.CONTAINS, SubString.CONTAINS),
                    pair(GraqlToken.Predicate.SubString.LIKE, SubString.LIKE)
            );

            private static Operator.SubString of(GraqlToken.Predicate.SubString token) {
                return operators.get(token);
            }
        }
    }

    public static abstract class Value<VAL_OPERATOR extends Predicate.Operator> extends Predicate<VAL_OPERATOR> {

        protected Value(VAL_OPERATOR operator) {
            super(operator);
        }

        public abstract boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value);

        public static class Boolean extends Predicate.Value<Operator.Equality> {

            public Boolean(GraqlToken.Predicate.Equality token) {
                super(Operator.Equality.of(token));
            }

            @Override
            public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.BOOLEAN)) return false;
                assert vertex.isBoolean() && value.isBoolean();

                return vertex.asBoolean().value() == value.getBoolean();
            }
        }

        public static class Long extends Predicate.Value<Operator.Equality> {

            public Long(GraqlToken.Predicate.Equality token) {
                super(Operator.Equality.of(token));
            }

            @Override
            public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.LONG)) return false;
                assert (vertex.isLong() || vertex.isDouble()) && value.isLong();

                if (vertex.isLong()) return operator.apply(vertex.asLong().value().compareTo(value.getLong()));
                else if (vertex.isDouble())
                    return operator.apply(Double.compare(vertex.asDouble().value(), value.getLong()));
                else throw GraknException.of(ILLEGAL_STATE);
            }
        }

        public static class Double extends Predicate.Value<Operator.Equality> {

            // TODO: where should we save this constant? Encoding?
            private static final double PRECISION = 0.0000000000000001;

            public Double(GraqlToken.Predicate.Equality token) {
                super(Operator.Equality.of(token));
            }

            @Override
            public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DOUBLE)) return false;
                assert (vertex.isLong() || vertex.isDouble()) && value.isDouble();

                double vertexValue;
                if (vertex.isLong()) vertexValue = vertex.asLong().value();
                else if (vertex.isDouble()) vertexValue = vertex.asDouble().value();
                else throw GraknException.of(ILLEGAL_STATE);
                return operator.apply(java.lang.Double.compare(vertexValue, value.getDouble()));
            }

            private static int compare(double vertexValue, double value) {
                int res = java.lang.Double.compare(vertexValue, value);
                if (res == 0) return 0;
                else if (Math.abs(vertexValue - value) < PRECISION) return 0;
                else return res;
            }
        }

        public static class DateTime extends Predicate.Value<Operator.Equality> {

            public DateTime(GraqlToken.Predicate.Equality token) {
                super(Operator.Equality.of(token));
            }

            @Override
            public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.DATETIME)) return false;
                assert vertex.isDateTime() && value.isDateTime();

                return operator.apply(vertex.asDateTime().value().compareTo(value.getDateTime()));
            }
        }

        public static class String extends Predicate.Value<Operator> {

            public String(GraqlToken.Predicate token) {
                super(Operator.of(token));
            }

            @Override
            public boolean apply(AttributeVertex<?> vertex, Traversal.Parameters.Value value) {
                if (!vertex.valueType().comparableTo(Encoding.ValueType.STRING)) return false;
                assert vertex.isString();

                if (operator.isEquality()) {
                    assert value.isString();
                    return operator.asEquality().apply(vertex.asString().value().compareTo(value.getString()));
                } else if (operator.isSubString()) {
                    return operator.asSubString().apply(vertex.asString().value(), value);
                } else {
                    throw GraknException.of(ILLEGAL_STATE);
                }
            }
        }
    }

    public static class Variable extends Predicate<Predicate.Operator.Equality> {

        public Variable(GraqlToken.Predicate.Equality token) {
            this(Operator.Equality.of(token));
        }

        private Variable(Operator.Equality operator) {
            super(operator);
        }

        public boolean apply(AttributeVertex<?> from, AttributeVertex<?> to) {
            return false; // TODO
        }

        public Variable reflection() {
            return new Variable(operator.reflection());
        }
    }
}
