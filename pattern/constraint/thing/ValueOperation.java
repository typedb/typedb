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
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.common.GraqlToken;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Query.INVALID_CASTING;

public abstract class ValueOperation<T> {

    private final GraqlToken.Comparator comparator;
    private final T value;
    private final int hash;

    ValueOperation(final GraqlToken.Comparator comparator, final T value) {
        this.comparator = comparator;
        this.value = value;
        this.hash = Objects.hash(this.comparator, this.value);
    }

    public static ValueOperation<?> of(final graql.lang.pattern.constraint.ValueOperation<?> constraint,
                                       final VariableRegistry registry) {
        if (constraint.isAssignment()) return ValueOperation.Assignment.of(constraint.asAssignment());
        else if (constraint.isComparison()) return ValueOperation.Comparison.of(constraint.asComparison(), registry);
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public GraqlToken.Comparator comparator() {
        return comparator;
    }

    public T value() {
        return value;
    }

    public ValueOperation.Assignment<?> asAssignment() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Assignment.class)));
    }

    public ValueOperation.Comparison<?> asComparison() {
        throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Comparison.class)));
    }

    public boolean isAssignment() {
        return false;
    }

    public boolean isComparison() {
        return false;
    }

    public boolean isValueEquality() {
        return comparator.equals(GraqlToken.Comparator.EQV) && !variable().isPresent();
    }

    public Optional<ThingVariable> variable() {
        return Optional.empty();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ValueOperation<?> that = (ValueOperation<?>) o;
        return (this.comparator.equals(that.comparator) && this.value.equals(that.value));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public abstract static class Assignment<T> extends ValueOperation<T> {

        Assignment(final T value) {
            super(GraqlToken.Comparator.EQV, value);
        }

        public static Assignment<?> of(final graql.lang.pattern.constraint.ValueOperation.Assignment<?> assignment) {
            if (assignment.isLong()) return new Assignment.Long(assignment.asLong().value());
            else if (assignment.isDouble()) return new Assignment.Double(assignment.asDouble().value());
            else if (assignment.isBoolean()) return new Assignment.Boolean(assignment.asBoolean().value());
            else if (assignment.isString()) return new Assignment.String(assignment.asString().value());
            else if (assignment.isDateTime()) return new Assignment.DateTime(assignment.asDateTime().value());
            else throw GraknException.of(ILLEGAL_STATE);
        }

        @Override
        public boolean isAssignment() {
            return true;
        }

        @Override
        public ValueOperation.Assignment<?> asAssignment() {
            return this;
        }

        public Assignment.Long asLong() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Long.class)));
        }

        public Assignment.Double asDouble() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Double.class)));
        }

        public Assignment.Boolean asBoolean() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Boolean.class)));
        }

        public Assignment.String asString() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(String.class)));
        }

        public Assignment.DateTime asDateTime() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(DateTime.class)));
        }

        public static class Long extends Assignment<java.lang.Long> {

            public Long(final long value) {
                super(value);
            }

            @Override
            public Assignment.Long asLong() {
                return this;
            }

            @Override
            public Double asDouble() {
                return new Assignment.Double(value());
            }
        }

        public static class Double extends Assignment<java.lang.Double> {

            public Double(final double value) {
                super(value);
            }

            @Override
            public Assignment.Double asDouble() {
                return this;
            }
        }

        public static class Boolean extends Assignment<java.lang.Boolean> {

            public Boolean(final boolean value) {
                super(value);
            }

            @Override
            public Assignment.Boolean asBoolean() {
                return this;
            }
        }

        public static class String extends Assignment<java.lang.String> {

            public String(final java.lang.String value) {
                super(value);
            }

            @Override
            public Assignment.String asString() {
                return this;
            }
        }

        public static class DateTime extends Assignment<LocalDateTime> {

            public DateTime(final LocalDateTime value) {
                super(value);
            }

            @Override
            public Assignment.DateTime asDateTime() {
                return this;
            }
        }
    }

    public abstract static class Comparison<T> extends ValueOperation<T> {

        Comparison(final GraqlToken.Comparator comparator, final T value) {
            super(comparator, value);
        }

        public static Comparison<?> of(final graql.lang.pattern.constraint.ValueOperation.Comparison<?> comparison, final VariableRegistry register) {
            if (comparison.isLong()) {
                return new Comparison.Long(comparison.comparator(), comparison.asLong().value());
            } else if (comparison.isDouble()) {
                return new Double(comparison.comparator(), comparison.asDouble().value());
            } else if (comparison.isBoolean()) {
                return new Boolean(comparison.comparator(), comparison.asBoolean().value());
            } else if (comparison.isString()) {
                return new String(comparison.comparator(), comparison.asString().value());
            } else if (comparison.isDateTime()) {
                return new DateTime(comparison.comparator(), comparison.asDateTime().value());
            } else if (comparison.isVariable()) {
                assert comparison.variable().isPresent();
                return new Comparison.Variable(comparison.comparator(), register.register(comparison.variable().get()));
            } else throw GraknException.of(ILLEGAL_STATE);
        }

        @Override
        public boolean isComparison() {
            return true;
        }

        @Override
        public ValueOperation.Comparison<?> asComparison() {
            return this;
        }

        public Comparison.Long asLong() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Long.class)));
        }

        public Comparison.Double asDouble() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Double.class)));
        }

        public Comparison.Boolean asBoolean() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Boolean.class)));
        }

        public Comparison.String asString() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(String.class)));
        }

        public Comparison.DateTime asDateTime() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(DateTime.class)));
        }

        public Comparison.Variable asVariable() {
            throw GraknException.of(INVALID_CASTING.message(className(this.getClass()), className(Variable.class)));
        }

        public static class Long extends Comparison<java.lang.Long> {

            public Long(final GraqlToken.Comparator comparator, final long value) {
                super(comparator, value);
            }

            @Override
            public Comparison.Long asLong() {
                return this;
            }
        }

        public static class Double extends Comparison<java.lang.Double> {

            public Double(final GraqlToken.Comparator comparator, final double value) {
                super(comparator, value);
            }

            @Override
            public Comparison.Double asDouble() {
                return this;
            }
        }

        public static class Boolean extends Comparison<java.lang.Boolean> {

            public Boolean(final GraqlToken.Comparator comparator, final boolean value) {
                super(comparator, value);
            }

            @Override
            public Comparison.Boolean asBoolean() {
                return this;
            }
        }

        public static class String extends Comparison<java.lang.String> {

            public String(final GraqlToken.Comparator comparator, final java.lang.String value) {
                super(comparator, value);
            }

            @Override
            public Comparison.String asString() {
                return this;
            }
        }

        public static class DateTime extends Comparison<LocalDateTime> {

            public DateTime(final GraqlToken.Comparator comparator, final LocalDateTime value) {
                super(comparator, value);
            }

            @Override
            public Comparison.DateTime asDateTime() {
                return this;
            }
        }

        public static class Variable extends Comparison<ThingVariable> {

            public Variable(final GraqlToken.Comparator comparator, final ThingVariable variable) {
                super(comparator, variable);
            }

            @Override
            public Optional<ThingVariable> variable() {
                return Optional.of(value());
            }

            @Override
            public Comparison.Variable asVariable() {
                return this;
            }
        }
    }
}
