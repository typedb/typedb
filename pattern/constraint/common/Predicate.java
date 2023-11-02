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

package com.vaticle.typedb.core.pattern.constraint.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.predicate.PredicateOperator;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.common.TypeQLVariable;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.MISSING_CONSTRAINT_VALUE;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.QUOTE_DOUBLE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.Equality.EQ;

public abstract class Predicate<T> implements AlphaEquivalent<Predicate<?>> {

    final TypeQLToken.Predicate predicate;
    final T value;
    private final int hash;

    private Predicate(TypeQLToken.Predicate predicate, T value) {
        assert !predicate.isEquality() || value instanceof Comparable ||
                value instanceof ThingVariable || value instanceof ValueVariable;
        assert !predicate.isSubString() || value instanceof java.lang.String;
        if (value == null) throw TypeDBException.of(MISSING_CONSTRAINT_VALUE);
        this.predicate = predicate;
        this.value = value;
        this.hash = Objects.hash(this.predicate, this.value);
    }

    public static Predicate<?> of(com.vaticle.typeql.lang.pattern.constraint.Predicate<?> predicate, VariableRegistry register) {
        if (predicate.isLong()) {
            return new Constant.Long(predicate.predicate().asEquality(), predicate.asLong().value());
        } else if (predicate.isDouble()) {
            return new Constant.Double(predicate.predicate().asEquality(), predicate.asDouble().value());
        } else if (predicate.isBoolean()) {
            return new Constant.Boolean(predicate.predicate().asEquality(), predicate.asBoolean().value());
        } else if (predicate.isString()) {
            return new Constant.String(predicate.predicate(), predicate.asString().value());
        } else if (predicate.isDateTime()) {
            return new Constant.DateTime(predicate.predicate().asEquality(), predicate.asDateTime().value());
        } else if (predicate.isVariable()) {
            TypeQLVariable rhs = predicate.asVariable().value();
            if (rhs.isConceptVar()) {
                return new ThingVar(predicate.predicate().asEquality(), register.registerThingVariable(rhs.asConceptVar()));
            } else if (rhs.isValueVar()) {
                return new ValueVar(predicate.predicate().asEquality(), register.registerValueVariable(rhs.asValueVar()));
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static Predicate<?> of(Predicate<?> toClone, VariableCloner cloner) {
        if (toClone.isConstant()) {
            if (toClone.asConstant().isLong()) {
                return new Constant.Long(toClone.predicate().asEquality(), toClone.asConstant().asLong().value());
            } else if (toClone.asConstant().isDouble()) {
                return new Constant.Double(toClone.predicate().asEquality(), toClone.asConstant().asDouble().value());
            } else if (toClone.asConstant().isBoolean()) {
                return new Constant.Boolean(toClone.predicate().asEquality(), toClone.asConstant().asBoolean().value());
            } else if (toClone.asConstant().isString()) {
                return new Constant.String(toClone.predicate(), toClone.asConstant().asString().value());
            } else if (toClone.asConstant().isDateTime()) {
                return new Constant.DateTime(toClone.predicate().asEquality(), toClone.asConstant().asDateTime().value());
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else if (toClone.isThingVar()) {
            return new ThingVar(toClone.predicate().asEquality(), cloner.clone(toClone.asThingVar().value()));
        } else if (toClone.isValueVar()) {
            return new ValueVar(toClone.predicate().asEquality(), cloner.clone(toClone.asValueVar().value()));
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public TypeQLToken.Predicate predicate() {
        return predicate;
    }

    public T value() {
        return value;
    }

    public boolean isValueIdentity() {
        return predicate.equals(EQ) && isConstant();
    }

    public boolean isThingVar() {
        return false;
    }

    public boolean isValueVar() {
        return false;
    }

    public boolean isConstant() {
        return false;
    }

    public Constant<T> asConstant() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.class));
    }

    public ThingVar asThingVar() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ThingVar.class));
    }

    public ValueVar asValueVar() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(ValueVar.class));
    }

    public abstract Predicate<T> clone(Conjunction.ConstraintCloner cloner);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Predicate<?> that = (Predicate<?>) o;
        return this.predicate.equals(that.predicate) && this.value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public java.lang.String toString() {
        return predicate.toString() + SPACE + value.toString();
    }

    public Set<Variable> variables() {
        return set();
    }


    public <TYPE> boolean inconsistentWith(Predicate<TYPE> predicate) {
        if (isValueVar() || isThingVar() || predicate.isThingVar() || predicate.isValueVar()) return false;
        return !asConstant().isConsistentWith(predicate.asConstant());
    }

    public static abstract class Constant<VALUE_TYPE> extends Predicate<VALUE_TYPE> {

        final Encoding.ValueType<VALUE_TYPE> valueType;

        private Constant(TypeQLToken.Predicate predicate, VALUE_TYPE value, Encoding.ValueType<VALUE_TYPE> valueType) {
            super(predicate, value);
            this.valueType = valueType;
        }

        public Encoding.ValueType<VALUE_TYPE> valueType() {
            return valueType;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public Constant<VALUE_TYPE> asConstant() {
            return this;
        }

        public boolean isLong() {
            return false;
        }

        public boolean isDouble() {
            return false;
        }

        public boolean isBoolean() {
            return false;
        }

        public boolean isString() {
            return false;
        }

        public boolean isDateTime() {
            return false;
        }

        public Constant.Long asLong() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.Long.class));
        }

        public Constant.Double asDouble() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.Double.class));
        }

        public Constant.Boolean asBoolean() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.Boolean.class));
        }

        public Constant.String asString() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.String.class));
        }

        public Constant.DateTime asDateTime() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.DateTime.class));
        }

        abstract <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other);

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(Predicate<?> that) {
            return iterate(AlphaEquivalence.empty())
                    .flatMap(a -> a.alphaEqualIf(isConstant() && that.isConstant()))
                    .flatMap(a -> a.alphaEqualIf(this.predicate.equals(that.predicate)))
                    .flatMap(a -> a.alphaEqualIf(this.value.equals(that.value)));
        }

        public static class Long extends Constant<java.lang.Long> {

            public Long(TypeQLToken.Predicate.Equality predicate, long value) {
                super(predicate, value, Encoding.ValueType.LONG);
            }

            @Override
            public TypeQLToken.Predicate.Equality predicate() {
                return super.predicate().asEquality();
            }

            @Override
            public boolean isLong() {
                return true;
            }

            @Override
            public Long asLong() {
                return this;
            }

            @Override
            public Double asDouble() {
                return new Double(predicate.asEquality(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                return com.vaticle.typedb.core.traversal.predicate.Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.LONG)
                        .apply(other.valueType(), other.value(), value);
            }

            @Override
            public Long clone(Conjunction.ConstraintCloner cloner) {
                return new Long(predicate(), value);
            }
        }

        public static class Double extends Constant<java.lang.Double> {

            public Double(TypeQLToken.Predicate.Equality predicate, double value) {
                super(predicate, value, Encoding.ValueType.DOUBLE);
            }

            @Override
            public TypeQLToken.Predicate.Equality predicate() {
                return super.predicate().asEquality();
            }

            @Override
            public boolean isDouble() {
                return true;
            }

            @Override
            public Double asDouble() {
                return this;
            }

            @Override
            public <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                return com.vaticle.typedb.core.traversal.predicate.Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.DOUBLE)
                        .apply(other.valueType(), other.value(), value);
            }

            @Override
            public Double clone(Conjunction.ConstraintCloner cloner) {
                return new Double(predicate(), value);
            }

        }

        public static class Boolean extends Constant<java.lang.Boolean> {

            public Boolean(TypeQLToken.Predicate.Equality predicate, boolean value) {
                super(predicate, value, Encoding.ValueType.BOOLEAN);
            }

            @Override
            public TypeQLToken.Predicate.Equality predicate() {
                return super.predicate().asEquality();
            }

            @Override
            public boolean isBoolean() {
                return true;
            }

            @Override
            public Boolean asBoolean() {
                return this;
            }

            @Override
            public <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                return com.vaticle.typedb.core.traversal.predicate.Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.BOOLEAN)
                        .apply(other.valueType(), other.value(), value);
            }

            @Override
            public Boolean clone(Conjunction.ConstraintCloner cloner) {
                return new Boolean(predicate(), value);
            }

        }

        public static class String extends Constant<java.lang.String> {

            public String(TypeQLToken.Predicate predicate, java.lang.String value) {
                super(predicate, value, Encoding.ValueType.STRING);
            }

            @Override
            public boolean isString() {
                return true;
            }

            @Override
            public String asString() {
                return this;
            }

            @Override
            public java.lang.String toString() {
                return predicate.toString() + SPACE + QUOTE_DOUBLE + value + QUOTE_DOUBLE;
            }

            @Override
            public <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueType.comparableTo(other.valueType)) return false;
                assert other.isString();
                if (predicate().isEquality()) {
                    return com.vaticle.typedb.core.traversal.predicate.Predicate.Value.String.of(predicate).apply(other.valueType(), other.value(), value);
                } else if (predicate.isSubString()) {
                    PredicateOperator.SubString<?> operator = PredicateOperator.SubString.of(predicate.asSubString());
                    if (operator == PredicateOperator.SubString.CONTAINS) {
                        return PredicateOperator.SubString.CONTAINS.apply(other.asString().value, value);
                    } else if (operator == PredicateOperator.SubString.LIKE) {
                        return PredicateOperator.SubString.LIKE.apply(other.asString().value, Pattern.compile(value));
                    } else throw TypeDBException.of(ILLEGAL_STATE);
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }

            @Override
            public String clone(Conjunction.ConstraintCloner cloner) {
                return new String(predicate(), value);
            }

        }

        public static class DateTime extends Constant<LocalDateTime> {

            public DateTime(TypeQLToken.Predicate.Equality predicate, LocalDateTime value) {
                super(predicate, value, Encoding.ValueType.DATETIME);
            }

            @Override
            public TypeQLToken.Predicate.Equality predicate() {
                return super.predicate().asEquality();
            }

            @Override
            public boolean isDateTime() {
                return true;
            }

            @Override
            public DateTime asDateTime() {
                return this;
            }

            @Override
            public <TYPE> boolean isConsistentWith(Predicate.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                return com.vaticle.typedb.core.traversal.predicate.Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.DATETIME)
                        .apply(other.valueType(), other.value(), value);
            }

            @Override
            public DateTime clone(Conjunction.ConstraintCloner cloner) {
                return new DateTime(predicate(), value);
            }

        }
    }

    public static class ThingVar extends Predicate<ThingVariable> {

        public ThingVar(TypeQLToken.Predicate.Equality predicate, ThingVariable variable) {
            super(predicate, variable);
        }

        @Override
        public TypeQLToken.Predicate.Equality predicate() {
            return predicate.asEquality();
        }

        @Override
        public boolean isThingVar() {
            return true;
        }

        @Override
        public ThingVar asThingVar() {
            return this;
        }

        @Override
        public Set<Variable> variables() {
            return set(value);
        }

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(Predicate<?> that) {
            return that.isThingVar() ? value.alphaEquals(that.asThingVar().value) : Iterators.empty();
        }

        @Override
        public ThingVar clone(Conjunction.ConstraintCloner cloner) {
            return new ThingVar(predicate(), cloner.cloneVariable(value));
        }
    }

    public static class ValueVar extends Predicate<ValueVariable> {

        public ValueVar(TypeQLToken.Predicate.Equality predicate, ValueVariable variable) {
            super(predicate, variable);
        }

        @Override
        public TypeQLToken.Predicate.Equality predicate() {
            return predicate.asEquality();
        }

        @Override
        public boolean isValueVar() {
            return true;
        }

        @Override
        public ValueVar asValueVar() {
            return this;
        }

        @Override
        public Set<Variable> variables() {
            return set(value);
        }

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(Predicate<?> that) {
            return that.isValueVar() ? value.alphaEquals(that.asValueVar().value) : Iterators.empty();
        }

        @Override
        public ValueVar clone(Conjunction.ConstraintCloner cloner) {
            return new ValueVar(predicate(), cloner.cloneVariable(value));
        }
    }
}
