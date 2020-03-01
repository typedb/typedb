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

package grakn.core.core;

import grakn.core.kb.concept.api.AttributeType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import static grakn.common.util.Collections.map;
import static grakn.common.util.Collections.pair;

public abstract class AttributeSerialiser<DESERIALISED, SERIALISED> {

    public static final AttributeSerialiser<Boolean, Boolean> BOOLEAN = new Default<>();
    public static final AttributeSerialiser<Float, Float> FLOAT = new Default<>();
    public static final AttributeSerialiser<Double, Double> DOUBLE = new Default<>();
    public static final AttributeSerialiser<Integer, Integer> INTEGER = new Default<>();
    public static final AttributeSerialiser<Long, Long> LONG = new Default<>();
    public static final AttributeSerialiser<String, String> STRING = new Default<>();
    public static final AttributeSerialiser<LocalDateTime, Long> DATE = new AttributeSerialiser.Date();

    AttributeSerialiser() {}

    public abstract SERIALISED serialise(DESERIALISED value);

    public abstract DESERIALISED deserialise(SERIALISED value);

    private static Map<AttributeType.DataType<?>, AttributeSerialiser<?, ?>> serialisers = map(
            pair(AttributeType.DataType.BOOLEAN, BOOLEAN),
            pair(AttributeType.DataType.DATE, DATE),
            pair(AttributeType.DataType.DOUBLE, DOUBLE),
            pair(AttributeType.DataType.FLOAT, FLOAT),
            pair(AttributeType.DataType.INTEGER, INTEGER),
            pair(AttributeType.DataType.LONG, LONG),
            pair(AttributeType.DataType.STRING, STRING)
    );


    // TODO: This method should not be needed if all usage of this class is
    //       accessed via the constant properties defined above.
    public static <DESERIALISED> AttributeSerialiser<DESERIALISED, ?> of(AttributeType.DataType<DESERIALISED> dataType) {
        AttributeSerialiser<?, ?> attributeSerialiser = serialisers.get(dataType);
        if (attributeSerialiser == null){
            throw new UnsupportedOperationException("Unsupported DataType: " + dataType.toString());
        }
        return (AttributeSerialiser<DESERIALISED, ?>) attributeSerialiser;

    }

    public static class Default<VALUE> extends AttributeSerialiser<VALUE, VALUE> {

        @Override
        public VALUE serialise(VALUE value) {
            return value;
        }

        @Override
        public VALUE deserialise(VALUE value) {
            return  value;
        }
    }

    public static class Date extends AttributeSerialiser<LocalDateTime, Long> {

        @Override
        public java.lang.Long serialise(LocalDateTime value) {
            return value.atZone(ZoneId.of("Z")).toInstant().toEpochMilli();
        }

        @Override
        public LocalDateTime deserialise(Long value) {
            if (value == null) return null;
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.of("Z"));
        }
    }

}

