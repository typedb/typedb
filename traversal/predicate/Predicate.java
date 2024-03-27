/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.predicate;

import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.Objects;
import java.util.regex.Pattern;

public abstract class Predicate<PRED_OP extends PredicateOperator, PRED_ARG extends PredicateArgument> {

    final PRED_OP operator;
    final PRED_ARG argument;
    private final int hash;

    private Predicate(PRED_OP operator, PRED_ARG argument) {
        this.operator = operator;
        this.argument = argument;
        this.hash = Objects.hash(operator, argument);
    }

    public static boolean stringContains(String superString, String subString) {
        int len2 = subString.length();
        if (len2 == 0) return true; // Empty string is contained

        char first2Lo = Character.toLowerCase(subString.charAt(0));
        char first2Up = Character.toUpperCase(subString.charAt(0));

        for (int i = 0; i <= superString.length() - len2; i++) {
            // Quick check before calling the more expensive regionMatches() method:
            char first1 = superString.charAt(i);
            if (first1 != first2Lo && first1 != first2Up) continue;
            if (superString.regionMatches(true, i, subString, 0, len2)) return true;
        }
        return false;
    }

    public static boolean stringLike(Pattern regex, String value) {
        return regex.matcher(value).matches();
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

    public static abstract class Value<VAL_OP extends PredicateOperator, ARG_TYPE> extends Predicate<VAL_OP, PredicateArgument.Value<VAL_OP, ARG_TYPE>> {

        public Value(VAL_OP operator, PredicateArgument.Value<VAL_OP, ARG_TYPE> argument) {
            super(operator, argument);
        }

        public Encoding.ValueType<ARG_TYPE> valueType() {
            return argument.valueType();
        }

        public boolean apply(com.vaticle.typedb.core.graph.vertex.Value<?> vertex, Traversal.Parameters.Value<?> value) {
            return argument.apply(operator, vertex, value);
        }

        public <T> boolean apply(com.vaticle.typedb.core.graph.vertex.Value<T> vertex, ARG_TYPE rhs) {
            return apply(vertex.valueType(), vertex.value(), rhs);
        }

        public <T> boolean apply(Encoding.ValueType<T> lhsType, T lhs, ARG_TYPE rhs) {
            return argument.apply(operator, lhsType, lhs, rhs);
        }

        public static class Numerical<ARG_TYPE> extends Value<PredicateOperator.Equality, ARG_TYPE> {

            public Numerical(PredicateOperator.Equality operator, PredicateArgument.Value<PredicateOperator.Equality, ARG_TYPE> argument) {
                super(operator, argument);
            }

            public static <ARG> Numerical<ARG> of(TypeQLToken.Predicate.Equality token, PredicateArgument.Value<PredicateOperator.Equality, ARG> argument) {
                return new Numerical<>(PredicateOperator.Equality.of(token), argument);
            }
        }

        public static class String extends Value<PredicateOperator, java.lang.String> {

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

        public boolean apply(com.vaticle.typedb.core.graph.vertex.Value<?> fromVertex, com.vaticle.typedb.core.graph.vertex.Value<?> toVertex) {
            return argument.apply(operator, fromVertex, toVertex);
        }

        public Predicate.Variable reflection() {
            return new Predicate.Variable(operator.reflection());
        }
    }

}
