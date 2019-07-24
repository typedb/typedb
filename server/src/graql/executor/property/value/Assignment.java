/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.executor.property.value;

import grakn.core.concept.type.AttributeType;
import grakn.core.server.kb.concept.Serialiser;
import graql.lang.Graql;
import graql.lang.property.ValueProperty;
import java.time.LocalDateTime;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import static grakn.core.common.util.Collections.set;

public abstract class Assignment<T, U> extends Operation<T, U> {

    Assignment(T value) {
        super(Graql.Token.Comparator.EQV, value);
    }

    public static Assignment<?, ?> of(ValueProperty.Operation.Assignment<?> assignment) {
        if (assignment instanceof ValueProperty.Operation.Assignment.Number<?>) {
            return new Number<>(((ValueProperty.Operation.Assignment.Number<?>) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.Boolean) {
            return new Assignment.Boolean(((ValueProperty.Operation.Assignment.Boolean) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.String) {
            return new Assignment.String(((ValueProperty.Operation.Assignment.String) assignment).value());

        } else if (assignment instanceof ValueProperty.Operation.Assignment.DateTime) {
            return new Assignment.DateTime(((ValueProperty.Operation.Assignment.DateTime) assignment).value());

        } else {
            throw new UnsupportedOperationException("Unsupported Value Assignment: " + assignment.getClass());
        }
    }

    @Override
    protected P<U> predicate() {
        return P.eq(valueSerialised());
    }

    static class Number<N extends java.lang.Number> extends Assignment<N, N> {

        Number(N value) {
            super(value);
        }

        @Override
        protected Set<AttributeType.DataType<?>> comparableDataTypes() {
            return set(AttributeType.DataType.DOUBLE,
                    //AttributeType.DataType.FLOAT,
                    //AttributeType.DataType.INTEGER,
                    AttributeType.DataType.LONG);
        }

        @Override
        N valueSerialised() {
            return new Serialiser.Default<N>().serialise(value());
        }

        @Override
        BoundDefinition<N> operationBounds() {
            return (BoundDefinition<N>) new BoundDefinition.NumberBound();
        }
    }

    static class Boolean extends Assignment<java.lang.Boolean, java.lang.Boolean> {

        Boolean(boolean value) {
            super(value);
        }

        @Override
        protected Set<AttributeType.DataType<?>> comparableDataTypes() {
            return set(AttributeType.DataType.BOOLEAN);
        }

        @Override
        java.lang.Boolean valueSerialised() {
            return Serialiser.BOOLEAN.serialise(value());
        }

        @Override
        BoundDefinition<java.lang.Boolean> operationBounds() {
            return new BoundDefinition.BooleanBound();
        }
    }

    static class DateTime extends Assignment<LocalDateTime, Long> {

        DateTime(LocalDateTime value) {
            super(value);
        }

        @Override
        protected Set<AttributeType.DataType<?>> comparableDataTypes() {
            return set(AttributeType.DataType.DATE);
        }

        @Override
        Long valueSerialised() {
            return Serialiser.DATE.serialise(value());
        }

        @Override
        BoundDefinition<Long> operationBounds() {
            return new BoundDefinition.LongBound();
        }
    }

    static class String extends Assignment<java.lang.String, java.lang.String> {

        String(java.lang.String value) {
            super(value);
        }

        @Override
        protected Set<AttributeType.DataType<?>> comparableDataTypes() {
            return set(AttributeType.DataType.STRING);
        }

        @Override
        java.lang.String valueSerialised() {
            return Serialiser.STRING.serialise(value());
        }

        @Override
        BoundDefinition<java.lang.String> operationBounds() {
            return new BoundDefinition.StringBound();
        }
    }
}
