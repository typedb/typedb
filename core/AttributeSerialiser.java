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
    public static final AttributeSerialiser<Double, Double> DOUBLE = new Default<>();
    public static final AttributeSerialiser<Long, Long> LONG = new Default<>();
    public static final AttributeSerialiser<String, String> STRING = new Default<>();
    public static final AttributeSerialiser<LocalDateTime, Long> DATE = new AttributeSerialiser.Date();

    AttributeSerialiser() {}

    public abstract SERIALISED serialise(DESERIALISED value);

    public abstract DESERIALISED deserialise(SERIALISED value);

    private static Map<AttributeType.ValueType<?>, AttributeSerialiser<?, ?>> serialisers = map(
            pair(AttributeType.ValueType.BOOLEAN, BOOLEAN),
            pair(AttributeType.ValueType.DATETIME, DATE),
            pair(AttributeType.ValueType.DOUBLE, DOUBLE),
            pair(AttributeType.ValueType.LONG, LONG),
            pair(AttributeType.ValueType.STRING, STRING)
    );


    // TODO: This method should not be needed if all usage of this class is
    //       accessed via the constant properties defined above.
    public static <DESERIALISED> AttributeSerialiser<DESERIALISED, ?> of(AttributeType.ValueType<DESERIALISED> valueType) {
        AttributeSerialiser<?, ?> attributeSerialiser = serialisers.get(valueType);
        if (attributeSerialiser == null){
            throw new UnsupportedOperationException("Unsupported ValueType: " + valueType.toString());
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

