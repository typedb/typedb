/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.predicate;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.Map;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class PredicateOperator {

    private final TypeQLToken.Predicate token;

    protected PredicateOperator(TypeQLToken.Predicate token) {
        this.token = token;
    }

    public static PredicateOperator of(TypeQLToken.Predicate token) {
        if (token.isEquality()) return Equality.of(token.asEquality());
        else if (token.isSubString()) return SubString.of(token.asSubString());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public boolean isEquality() {
        return false;
    }

    boolean isSubString() {
        return false;
    }

    Equality asEquality() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Equality.class));
    }

    SubString<?> asSubString() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(SubString.class));
    }

    @Override
    public String toString() {
        return token.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PredicateOperator that = (PredicateOperator) o;
        return this.token.equals(that.token);
    }

    @Override
    public int hashCode() {
        return token.hashCode();
    }

    public static abstract class Equality extends PredicateOperator {

        public Equality(TypeQLToken.Predicate.Equality token) {
            super(token);
        }

        public static Equality of(TypeQLToken.Predicate.Equality operator) {
            return Equality.operators.get(operator);
        }

        public abstract boolean apply(int comparisonResult);

        abstract Equality reflection();

        @Override
        public boolean isEquality() {
            return true;
        }

        @Override
        Equality asEquality() {
            return this;
        }

        public static final Equality EQ = new Equality(TypeQLToken.Predicate.Equality.EQ) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult == 0;
            }

            @Override
            Equality reflection() {
                return this;
            }
        };

        public static final Equality NEQ = new Equality(TypeQLToken.Predicate.Equality.NEQ) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult != 0;
            }

            @Override
            Equality reflection() {
                return this;
            }
        };

        public static final Equality GT = new Equality(TypeQLToken.Predicate.Equality.GT) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult > 0;
            }

            @Override
            Equality reflection() {
                return LT;
            }
        };

        public static final Equality GTE = new Equality(TypeQLToken.Predicate.Equality.GTE) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult >= 0;
            }

            @Override
            Equality reflection() {
                return LTE;
            }
        };

        public static final Equality LT = new Equality(TypeQLToken.Predicate.Equality.LT) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult < 0;
            }

            @Override
            Equality reflection() {
                return GT;
            }
        };

        public static final Equality LTE = new Equality(TypeQLToken.Predicate.Equality.LTE) {
            @Override
            public boolean apply(int comparisonResult) {
                return comparisonResult <= 0;
            }

            @Override
            Equality reflection() {
                return GTE;
            }
        };

        private static final Map<TypeQLToken.Predicate.Equality, Equality> operators = map(
                pair(TypeQLToken.Predicate.Equality.EQ, Equality.EQ),
                pair(TypeQLToken.Predicate.Equality.NEQ, Equality.NEQ),
                pair(TypeQLToken.Predicate.Equality.GT, Equality.GT),
                pair(TypeQLToken.Predicate.Equality.GTE, Equality.GTE),
                pair(TypeQLToken.Predicate.Equality.LT, Equality.LT),
                pair(TypeQLToken.Predicate.Equality.LTE, Equality.LTE)
        );
    }

    public static abstract class SubString<PRED_VALUE> extends PredicateOperator {

        private SubString(TypeQLToken.Predicate.SubString token) {
            super(token);
        }

        public static SubString<?> of(TypeQLToken.Predicate.SubString token) {
            return operators.get(token);
        }

        abstract public boolean apply(String vertexValue, Traversal.Parameters.Value<?> predicateValue);

        abstract public boolean apply(String vertexValue, PRED_VALUE predicateValue);

        @Override
        boolean isSubString() {
            return true;
        }

        @Override
        SubString<?> asSubString() {
            return this;
        }

        public static final SubString<String> CONTAINS = new SubString<>(TypeQLToken.Predicate.SubString.CONTAINS) {
            @Override
            public boolean apply(String vertexValue, Traversal.Parameters.Value<?> predicateValue) {
                assert predicateValue.isString();
                return apply(vertexValue, predicateValue.asString().value());
            }

            @Override
            public boolean apply(String vertexValue, String predicateValue) {
                return Predicate.stringContains(vertexValue, predicateValue);
            }
        };

        public static final SubString<Pattern> LIKE = new SubString<>(TypeQLToken.Predicate.SubString.LIKE) {
            @Override
            public boolean apply(String vertexValue, Traversal.Parameters.Value<?> predicateValue) {
                assert predicateValue.isRegex();
                return apply(vertexValue, predicateValue.asRegex().pattern());
            }

            @Override
            public boolean apply(String vertexValue, Pattern predicateValue) {
                return Predicate.stringLike(predicateValue, vertexValue);
            }
        };

        private static final Map<TypeQLToken.Predicate.SubString, SubString<?>> operators = map(
                pair(TypeQLToken.Predicate.SubString.CONTAINS, SubString.CONTAINS),
                pair(TypeQLToken.Predicate.SubString.LIKE, SubString.LIKE)
        );
    }
}
