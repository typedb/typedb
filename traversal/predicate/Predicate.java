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

import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.Objects;

import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.DOUBLE_PRECISION;

public abstract class Predicate<PRED_OP extends PredicateOperator, PRED_ARG extends PredicateArgument> {

    final PRED_OP operator;
    final PRED_ARG argument;
    private final int hash;

    private Predicate(PRED_OP operator, PRED_ARG argument) {
        this.operator = operator;
        this.argument = argument;
        this.hash = Objects.hash(operator, argument);
    }

    public static int compareDoubles(double first, double second) {
        int res = Double.compare(first, second);
        if (res == 0) return 0;
        else if (Math.abs(first - second) < DOUBLE_PRECISION) return 0;
        else return res;
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

    public static abstract class Value<VAL_OP extends PredicateOperator> extends Predicate<VAL_OP, PredicateArgument.Value<VAL_OP, ?>> {

        public Value(VAL_OP operator, PredicateArgument.Value<VAL_OP, ?> argument) {
            super(operator, argument);
        }

        public Encoding.ValueType valueType() {
            return argument.valueType();
        }

        public boolean apply(AttributeVertex<?> vertex, GraphTraversal.Parameters.Value value) {
            return argument.apply(operator, vertex, value);
        }

        public static class Numerical extends Value<PredicateOperator.Equality> {

            public Numerical(PredicateOperator.Equality operator, PredicateArgument.Value<PredicateOperator.Equality, ?> argument) {
                super(operator, argument);
            }

            public static Numerical of(TypeQLToken.Predicate.Equality token, PredicateArgument.Value<PredicateOperator.Equality, ?> argument) {
                return new Numerical(PredicateOperator.Equality.of(token), argument);
            }
        }

        public static class String extends Value<PredicateOperator> {

            public String(PredicateOperator operator, PredicateArgument.Value<PredicateOperator, java.lang.String> argument) {
                super(operator, argument);
            }

            public static String of(TypeQLToken.Predicate token) {
                return new String(PredicateOperator.of(token), PredicateArgument.Value.STRING);
            }
        }
    }

    public static class Variable extends Predicate<PredicateOperator.Equality, PredicateArgument.Variable> {


        private Variable(PredicateOperator.Equality operator) {
            super(operator, PredicateArgument.Variable.VARIABLE);
        }

        public static Predicate.Variable of(TypeQLToken.Predicate.Equality token) {
            return new Predicate.Variable(PredicateOperator.Equality.of(token));
        }

        public boolean apply(AttributeVertex<?> fromVertex, AttributeVertex<?> toVertex) {
            return argument.apply(operator, fromVertex, toVertex);
        }

        public Predicate.Variable reflection() {
            return new Predicate.Variable(operator.reflection());
        }
    }

}
