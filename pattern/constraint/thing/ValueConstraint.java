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
 */

package grakn.core.pattern.constraint.thing;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.equivalence.AlphaEquivalent;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import graql.lang.common.GraqlToken;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.exception.ErrorMessage.Pattern.MISSING_CONSTRAINT_VALUE;
import static graql.lang.common.GraqlToken.Char.QUOTE;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Predicate.Equality.EQ;

public abstract class ValueConstraint<T> extends ThingConstraint implements AlphaEquivalent<ValueConstraint<?>> {

    final GraqlToken.Predicate predicate;
    final T value;
    private final int hash;

    private ValueConstraint(ThingVariable owner, GraqlToken.Predicate predicate, T value,
                            Set<grakn.core.pattern.variable.Variable> additionalVariables) {
        super(owner, additionalVariables);
        assert !predicate.isEquality() || value instanceof Comparable || value instanceof ThingVariable;
        assert !predicate.isSubString() || value instanceof java.lang.String;
        if (value == null) throw GraknException.of(MISSING_CONSTRAINT_VALUE);
        this.predicate = predicate;
        this.value = value;
        this.hash = Objects.hash(this.predicate, this.value);
    }

    static ValueConstraint<?> of(ThingVariable owner,
                                 graql.lang.pattern.constraint.ThingConstraint.Value<?> valueConstraint,
                                 VariableRegistry register) {
        if (valueConstraint.isLong()) {
            return new Long(owner, valueConstraint.predicate().asEquality(), valueConstraint.asLong().value());
        } else if (valueConstraint.isDouble()) {
            return new Double(owner, valueConstraint.predicate().asEquality(), valueConstraint.asDouble().value());
        } else if (valueConstraint.isBoolean()) {
            return new Boolean(owner, valueConstraint.predicate().asEquality(), valueConstraint.asBoolean().value());
        } else if (valueConstraint.isString()) {
            return new String(owner, valueConstraint.predicate(), valueConstraint.asString().value());
        } else if (valueConstraint.isDateTime()) {
            return new DateTime(owner, valueConstraint.predicate().asEquality(), valueConstraint.asDateTime().value());
        } else if (valueConstraint.isVariable()) {
            return new Variable(owner, valueConstraint.predicate().asEquality(), register.register(valueConstraint.asVariable().value()));
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public ValueConstraint<?> asValue() {
        return this;
    }

    public GraqlToken.Predicate predicate() {
        return predicate;
    }

    public T value() {
        return value;
    }

    public boolean isValueIdentity() {
        return predicate.equals(EQ) && !isVariable();
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

    public boolean isVariable() {
        return false;
    }

    public Long asLong() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Long.class));
    }

    public Double asDouble() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Double.class));
    }

    public Boolean asBoolean() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Boolean.class));
    }

    public String asString() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(String.class));
    }

    public DateTime asDateTime() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(DateTime.class));
    }

    public Variable asVariable() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Variable.class));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ValueConstraint<?> that = (ValueConstraint<?>) o;
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
        return predicate.toString() + SPACE + value.toString();
    }

    public AlphaEquivalence alphaEquals(ValueConstraint<?> that) {
        return AlphaEquivalence.valid()
                .validIf(isLong() == that.isLong())
                .validIf(isDouble() == that.isDouble())
                .validIf(isBoolean() == that.isBoolean())
                .validIf(isString() == that.isString())
                .validIf(isDateTime() == that.isDateTime())
                .validIf(!isVariable() && !that.isVariable())
                .validIf(this.predicate.equals(that.predicate))
                .validIf(this.value.equals(that.value));
    }

    public static class Long extends ValueConstraint<java.lang.Long> {

        public Long(ThingVariable owner, GraqlToken.Predicate.Equality predicate, long value) {
            super(owner, predicate, value, set());
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate.asEquality(), value);
        }
    }

    public static class Double extends ValueConstraint<java.lang.Double> {

        public Double(ThingVariable owner, GraqlToken.Predicate.Equality predicate, double value) {
            super(owner, predicate, value, set());
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate.asEquality(), value);
        }
    }

    public static class Boolean extends ValueConstraint<java.lang.Boolean> {

        public Boolean(ThingVariable owner, GraqlToken.Predicate.Equality predicate, boolean value) {
            super(owner, predicate, value, set());
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate.asEquality(), value);
        }
    }

    public static class String extends ValueConstraint<java.lang.String> {

        public String(ThingVariable owner, GraqlToken.Predicate predicate, java.lang.String value) {
            super(owner, predicate, value, set());
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate, value);
        }

        @Override
        public java.lang.String toString() {
            return predicate.toString() + SPACE + QUOTE + value + QUOTE;
        }
    }

    public static class DateTime extends ValueConstraint<LocalDateTime> {

        public DateTime(ThingVariable owner, GraqlToken.Predicate.Equality predicate, LocalDateTime value) {
            super(owner, predicate, value, set());
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate.asEquality(), value);
        }
    }

    public static class Variable extends ValueConstraint<ThingVariable> {

        public Variable(ThingVariable owner, GraqlToken.Predicate.Equality predicate, ThingVariable variable) {
            super(owner, predicate, variable, set(variable));
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
        public void addTo(Traversal traversal) {
            traversal.predicate(owner.identifier(), predicate.asEquality(), value.identifier());
        }

        @Override
        public AlphaEquivalence alphaEquals(ValueConstraint<?> that) {
            return AlphaEquivalence.valid()
                    .validIf(isVariable() && that.isVariable())
                    .validIf(this.predicate.equals(that.predicate))
                    .validIfAlphaEqual(this.value, that.asVariable().value);
        }
    }
}

