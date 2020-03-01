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

package grakn.core.graql.planning.gremlin.value;

import grakn.core.core.AttributeSerialiser;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.time.LocalDateTime;

public abstract class ValueAssignment<T, U> extends ValueOperation<T, U> {

    ValueAssignment(T value) {
        super(Graql.Token.Comparator.EQV, value);
    }

    public static ValueAssignment<?, ?> of(ValueProperty.Operation.Assignment<?> assignment) {
        if (assignment instanceof ValueProperty.Operation.Assignment.Number<?>) {
            return new Number<>(((ValueProperty.Operation.Assignment.Number<?>) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.Boolean) {
            return new ValueAssignment.Boolean(((ValueProperty.Operation.Assignment.Boolean) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.String) {
            return new ValueAssignment.String(((ValueProperty.Operation.Assignment.String) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.DateTime) {
            return new ValueAssignment.DateTime(((ValueProperty.Operation.Assignment.DateTime) assignment).value());

        } else {
            throw new UnsupportedOperationException("Unsupported Value Assignment: " + assignment.getClass());
        }
    }

    @Override
    protected P<U> predicate() {
        return P.eq(valueSerialised());
    }

    static class Number<N extends java.lang.Number> extends ValueAssignment<N, N> {

        Number(N value) {
            super(value);
        }

        @Override
        public N valueSerialised() {
            return new AttributeSerialiser.Default<N>().serialise(value());
        }
    }

    static class Boolean extends ValueAssignment<java.lang.Boolean, java.lang.Boolean> {

        Boolean(boolean value) {
            super(value);
        }

        @Override
        public java.lang.Boolean valueSerialised() {
            return AttributeSerialiser.BOOLEAN.serialise(value());
        }
    }

    static class DateTime extends ValueAssignment<LocalDateTime, Long> {

        DateTime(LocalDateTime value) {
            super(value);
        }

        @Override
        public Long valueSerialised() {
            return AttributeSerialiser.DATE.serialise(value());
        }
    }

    static class String extends ValueAssignment<java.lang.String, java.lang.String> {

        String(java.lang.String value) {
            super(value);
        }

        @Override
        public java.lang.String valueSerialised() {
            return AttributeSerialiser.STRING.serialise(value());
        }
    }
}
