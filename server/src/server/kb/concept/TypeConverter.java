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

package grakn.core.server.kb.concept;

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.type.AttributeType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

public abstract class TypeConverter<DESERIALISED, SERIALISED>  extends Serialiser<DESERIALISED, SERIALISED> {

    private static Map<AttributeType.DataType<?>, Serialiser<?, ?>> converters = ImmutableMap.<AttributeType.DataType<?>, Serialiser<?, ?>>builder()
            .put(AttributeType.DataType.BOOLEAN, BOOLEAN)
            .put(AttributeType.DataType.DATE, new DateConverter())
            .put(AttributeType.DataType.DOUBLE, new DoubleConverter())
            .put(AttributeType.DataType.FLOAT, new FloatConverter())
            .put(AttributeType.DataType.INTEGER, new IntegerConverter())
            .put(AttributeType.DataType.LONG, new LongConverter())
            .put(AttributeType.DataType.STRING, STRING)
            .build();


    public static <DESERIALISED> Serialiser<DESERIALISED, Object> of(AttributeType.DataType<DESERIALISED> dataType) {
        Serialiser<?, ?> converter = converters.get(dataType);
        if (converter == null){
            throw new UnsupportedOperationException("Unsupported DataType: " + dataType.toString());
        }
        return (Serialiser<DESERIALISED, Object>) converter;

    }

    public static class DateConverter extends Serialiser<LocalDateTime, Object> {
        @Override
        public Object serialise(LocalDateTime value) {
            return value.atZone(ZoneId.of("Z")).toInstant().toEpochMilli();
        }

        @Override
        public LocalDateTime deserialise(Object value) {
            if (value instanceof Long) return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneId.of("Z"));
            throw new ClassCastException();
        }
    }

    public static class DoubleConverter extends Serialiser<Double, Object> {
        @Override
        public Double deserialise(Object value) {
            if (value instanceof Number){
                return (((Number) value).doubleValue());
            }
            throw new ClassCastException();
        }

        @Override
        public Object serialise(Double value) {
            return value;
        }
    }

    public static class FloatConverter extends Serialiser<Float, Object> {
        @Override
        public Float deserialise(Object value) {
            if (value instanceof Number){
                return (((Number) value).floatValue());
            }
            throw new ClassCastException();
        }

        @Override
        public Object serialise(Float value) {
            return value;
        }
    }

    public static class IntegerConverter extends Serialiser<Integer, Object> {
        @Override
        public Integer deserialise(Object value) {
            if (value instanceof Number) {
                Number n = (Number) value;
                if ( n.floatValue() % 1 == 0) return n.intValue();
            }
            throw new ClassCastException();
        }

        @Override
        public Object serialise(Integer value) {
            return value;
        }
    }

    public static class LongConverter extends Serialiser<Long, Object> {
        @Override
        public Long deserialise(Object value) {
            if (value instanceof Number) {
                Number n = (Number) value;
                if ( n.floatValue() % 1 == 0) return n.longValue();
            }
            throw new ClassCastException();
        }

        @Override
        public Object serialise(Long value) {
            return value;
        }
    }
}
