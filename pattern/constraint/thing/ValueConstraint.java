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
 */

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.predicate.PredicateOperator;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.MISSING_CONSTRAINT_VALUE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.QUOTE_DOUBLE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.Equality.EQ;

public abstract class ValueConstraint<T> extends ThingConstraint implements AlphaEquivalent<ValueConstraint<?>> {

    final TypeQLToken.Predicate predicate;
    final T value;
    private final int hash;

    private ValueConstraint(ThingVariable owner, TypeQLToken.Predicate predicate, T value,
                            Set<com.vaticle.typedb.core.pattern.variable.Variable> additionalVariables) {
        super(owner, additionalVariables);
        assert !predicate.isEquality() || value instanceof Comparable || value instanceof ThingVariable;
        assert !predicate.isSubString() || value instanceof java.lang.String;
        if (value == null) throw TypeDBException.of(MISSING_CONSTRAINT_VALUE);
        this.predicate = predicate;
        this.value = value;
        this.hash = Objects.hash(this.predicate, this.value);
    }

    static ValueConstraint<?> of(ThingVariable owner,
                                 com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Value<?> valueConstraint,
                                 VariableRegistry register) {
        if (valueConstraint.isLong()) {
            return new Constant.Long(owner, valueConstraint.predicate().asEquality(), valueConstraint.asLong().value());
        } else if (valueConstraint.isDouble()) {
            return new Constant.Double(owner, valueConstraint.predicate().asEquality(), valueConstraint.asDouble().value());
        } else if (valueConstraint.isBoolean()) {
            return new Constant.Boolean(owner, valueConstraint.predicate().asEquality(), valueConstraint.asBoolean().value());
        } else if (valueConstraint.isString()) {
            return new Constant.String(owner, valueConstraint.predicate(), valueConstraint.asString().value());
        } else if (valueConstraint.isDateTime()) {
            return new Constant.DateTime(owner, valueConstraint.predicate().asEquality(), valueConstraint.asDateTime().value());
        } else if (valueConstraint.isVariable()) {
            return new Variable(owner, valueConstraint.predicate().asEquality(), register.register(valueConstraint.asVariable().value()));
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    static ValueConstraint<?> of(ThingVariable owner, ValueConstraint<?> clone, VariableCloner cloner) {
        if (clone.isConstant()) {
            if (clone.asConstant().isLong()) {
                return new Constant.Long(owner, clone.predicate().asEquality(), clone.asConstant().asLong().value());
            } else if (clone.asConstant().isDouble()) {
                return new Constant.Double(owner, clone.predicate().asEquality(), clone.asConstant().asDouble().value());
            } else if (clone.asConstant().isBoolean()) {
                return new Constant.Boolean(owner, clone.predicate().asEquality(), clone.asConstant().asBoolean().value());
            } else if (clone.asConstant().isString()) {
                return new Constant.String(owner, clone.predicate(), clone.asConstant().asString().value());
            } else if (clone.asConstant().isDateTime()) {
                return new Constant.DateTime(owner, clone.predicate().asEquality(), clone.asConstant().asDateTime().value());
            } else throw TypeDBException.of(ILLEGAL_STATE);
        } else if (clone.isVariable()) {
            return new Variable(owner, clone.predicate().asEquality(), cloner.clone(clone.asVariable().value()));
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueConstraint<?> asValue() {
        return this;
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

    public boolean isVariable() {
        return false;
    }

    public boolean isConstant() {
        return false;
    }

    public Constant<T> asConstant() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Constant.class));
    }

    public Variable asVariable() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Variable.class));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueConstraint<?> that = (ValueConstraint<?>) o;
        return (this.owner.equals(that.owner) &&
                this.predicate.equals(that.predicate) &&
                this.value.equals(that.value));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public java.lang.String toString() {
        return owner.toString() + SPACE + predicate.toString() + SPACE + value.toString();
    }

    public <TYPE> boolean inconsistentWith(ValueConstraint<TYPE> valueConstraint) {
        if (isVariable() || valueConstraint.isVariable()) return false;
        return !asConstant().isConsistentWith(valueConstraint.asConstant());
    }

    public static abstract class Constant<VALUE_TYPE> extends ValueConstraint<VALUE_TYPE> {

        final Encoding.ValueType<VALUE_TYPE> valueEncoding;

        private Constant(ThingVariable owner, TypeQLToken.Predicate predicate, VALUE_TYPE value,
                         Set<com.vaticle.typedb.core.pattern.variable.Variable> additionalVariables, Encoding.ValueType<VALUE_TYPE> valueEncoding) {
            super(owner, predicate, value, additionalVariables);
            this.valueEncoding = valueEncoding;
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

        public Long asLong() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Long.class));
        }

        public Double asDouble() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Double.class));
        }

        public Boolean asBoolean() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Boolean.class));
        }

        public String asString() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(String.class));
        }

        public DateTime asDateTime() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(DateTime.class));
        }

        abstract <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other);

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(ValueConstraint<?> that) {
            return owner.alphaEquals(that.owner)
                    .flatMap(a -> a.alphaEqualIf(isConstant() && that.isConstant()))
                    .flatMap(a -> a.alphaEqualIf(this.predicate.equals(that.predicate)))
                    .flatMap(a -> a.alphaEqualIf(this.value.equals(that.value)));
        }

        public static class Long extends Constant<java.lang.Long> {

            public Long(ThingVariable owner, TypeQLToken.Predicate.Equality predicate, long value) {
                super(owner, predicate, value, set(), Encoding.ValueType.LONG);
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
                return new Double(owner, predicate.asEquality(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueEncoding.comparableTo(other.valueEncoding)) return false;
                return Encoding.ValueType.compare(valueEncoding, value, other.valueEncoding, other.value) == 0;
                if (other.isDouble()) {
                    return Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.DOUBLE)
                            .apply(other.asDouble().value(), value.doubleValue());
                } else {
                    return Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.LONG)
                            .apply(other.asLong().value, value);
                }
            }

            @Override
            public void addTo(GraphTraversal.Thing traversal) {
                traversal.predicate(owner.id(), predicate.asEquality(), value);
            }

            @Override
            public Long clone(Conjunction.ConstraintCloner cloner) {
                return cloner.cloneVariable(owner).valueLong(predicate(), value);
            }
        }

        public static class Double extends Constant<java.lang.Double> {

            public Double(ThingVariable owner, TypeQLToken.Predicate.Equality predicate, double value) {
                super(owner, predicate, value, set(), Encoding.ValueType.DOUBLE);
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
            public void addTo(GraphTraversal.Thing traversal) {
                traversal.predicate(owner.id(), predicate.asEquality(), value);
            }

            @Override
            public Double clone(Conjunction.ConstraintCloner cloner) {
                return cloner.cloneVariable(owner).valueDouble(predicate(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueEncoding.comparableTo(other.valueEncoding)) return false;
                return Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.DOUBLE)
                        .apply(other.asDouble().value(), value);
            }
        }

        public static class Boolean extends Constant<java.lang.Boolean> {

            public Boolean(ThingVariable owner, TypeQLToken.Predicate.Equality predicate, boolean value) {
                super(owner, predicate, value, set(), Encoding.ValueType.BOOLEAN);
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
            public void addTo(GraphTraversal.Thing traversal) {
                traversal.predicate(owner.id(), predicate.asEquality(), value);
            }

            @Override
            public Boolean clone(Conjunction.ConstraintCloner cloner) {
                return cloner.cloneVariable(owner).valueBoolean(predicate(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueEncoding.comparableTo(other.valueEncoding)) return false;
                assert other.isBoolean();
                return Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.BOOLEAN)
                        .apply(other.asBoolean().value, value);
            }
        }

        public static class String extends Constant<java.lang.String> {

            public String(ThingVariable owner, TypeQLToken.Predicate predicate, java.lang.String value) {
                super(owner, predicate, value, set(), Encoding.ValueType.STRING);
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
            public void addTo(GraphTraversal.Thing traversal) {
                traversal.predicate(owner.id(), predicate, value);
            }

            @Override
            public java.lang.String toString() {
                return owner.toString() + SPACE + predicate.toString() + SPACE + QUOTE_DOUBLE + value + QUOTE_DOUBLE;
            }

            @Override
            public String clone(Conjunction.ConstraintCloner cloner) {
                return cloner.cloneVariable(owner).valueString(predicate(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueEncoding.comparableTo(other.valueEncoding)) return false;
                assert other.isString();
                if (predicate().isEquality()) {
                    return Predicate.Value.String.of(predicate).apply(other.asString().value, value);
                } else if (predicate.isSubString()) {
                    PredicateOperator.SubString<?> operator = PredicateOperator.SubString.of(predicate.asSubString());
                    if (operator == PredicateOperator.SubString.CONTAINS) {
                        return PredicateOperator.SubString.CONTAINS.apply(other.asString().value, value);
                    } else if (operator == PredicateOperator.SubString.LIKE) {
                        return PredicateOperator.SubString.LIKE.apply(other.asString().value, Pattern.compile(value));
                    } else throw TypeDBException.of(ILLEGAL_STATE);
                } else throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        public static class DateTime extends Constant<LocalDateTime> {

            public DateTime(ThingVariable owner, TypeQLToken.Predicate.Equality predicate, LocalDateTime value) {
                super(owner, predicate, value, set(), Encoding.ValueType.DATETIME);
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
            public void addTo(GraphTraversal.Thing traversal) {
                traversal.predicate(owner.id(), predicate.asEquality(), value);
            }

            @Override
            public DateTime clone(Conjunction.ConstraintCloner cloner) {
                return cloner.cloneVariable(owner).valueDateTime(predicate(), value);
            }

            @Override
            public <TYPE> boolean isConsistentWith(ValueConstraint.Constant<TYPE> other) {
                if (other.predicate() != EQ) return true;
                if (!valueEncoding.comparableTo(other.valueEncoding)) return false;
                assert other.isDateTime();
                return Predicate.Value.Numerical.of(predicate.asEquality(), PredicateArgument.Value.DATETIME)
                        .apply(other.asDateTime().value, value);
            }
        }
    }

    public static class Variable extends ValueConstraint<ThingVariable> {

        public Variable(ThingVariable owner, TypeQLToken.Predicate.Equality predicate, ThingVariable variable) {
            super(owner, predicate, variable, set(variable));
            variable.constraining(this);
        }

        @Override
        public TypeQLToken.Predicate.Equality predicate() {
            return predicate.asEquality();
        }

        @Override
        public boolean isVariable() {
            return true;
        }

        @Override
        public Variable asVariable() {
            return this;
        }

        @Override
        public void addTo(GraphTraversal.Thing traversal) {
            traversal.predicate(owner.id(), predicate.asEquality(), value.id());
        }

        @Override
        public FunctionalIterator<AlphaEquivalence> alphaEquals(ValueConstraint<?> that) {
            return owner.alphaEquals(that.owner)
                    .flatMap(a -> a.alphaEqualIf(isVariable() && that.isVariable()))
                    .flatMap(a -> value.alphaEquals(that.asVariable().value).flatMap(a::extendIfCompatible))
                    .flatMap(a -> a.alphaEqualIf(this.predicate.equals(that.predicate)))
                    .flatMap(a -> this.value.alphaEquals(that.asVariable().value()).flatMap(a::extendIfCompatible));
        }

        @Override
        public Variable clone(Conjunction.ConstraintCloner cloner) {
            return cloner.cloneVariable(owner).valueVariable(predicate(), cloner.cloneVariable(value));
        }
    }
}

