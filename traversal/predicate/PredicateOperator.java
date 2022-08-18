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

    boolean isEquality() {
        return false;
    }

    boolean isSubString() {
        return false;
    }

    Equality asEquality() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Equality.class));
    }

    SubString asSubString() {
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
        boolean isEquality() {
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

        abstract public boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue);

        abstract public boolean apply(String vertexValue, PRED_VALUE predicateValue);

        @Override
        boolean isSubString() {
            return true;
        }

        @Override
        SubString asSubString() {
            return this;
        }

        public static final SubString CONTAINS = new SubString<String>(TypeQLToken.Predicate.SubString.CONTAINS) {
            @Override
            public boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue) {
                assert predicateValue.isString();
                return apply(vertexValue, predicateValue.getString());
            }

            @Override
            public boolean apply(String vertexValue, String predicateValue) {
                return Predicate.stringContains(vertexValue, predicateValue);
            }
        };

        public static final SubString LIKE = new SubString<Pattern>(TypeQLToken.Predicate.SubString.LIKE) {
            @Override
            public boolean apply(String vertexValue, Traversal.Parameters.Value predicateValue) {
                assert predicateValue.isRegex();
                return apply(vertexValue, predicateValue.getRegex());
            }

            @Override
            public boolean apply(String vertexValue, Pattern predicateValue) {
                return Predicate.stringLike(predicateValue, vertexValue);
            }
        };

        private static final Map<TypeQLToken.Predicate.SubString, SubString> operators = map(
                pair(TypeQLToken.Predicate.SubString.CONTAINS, SubString.CONTAINS),
                pair(TypeQLToken.Predicate.SubString.LIKE, SubString.LIKE)
        );
    }
}
