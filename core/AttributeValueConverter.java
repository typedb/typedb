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

import com.google.common.collect.ImmutableMap;
import grakn.core.kb.concept.api.AttributeType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Convert Attribute Values obtained from Graql and stored in Properties, and convert
 * them to native java data instances
 */
public abstract class AttributeValueConverter<SOURCE, TARGET>{

    private static Map<AttributeType.DataType<?>, AttributeValueConverter<?, ?>> converters = ImmutableMap.<AttributeType.DataType<?>, AttributeValueConverter<?, ?>>builder()
            .put(AttributeType.DataType.BOOLEAN, new IdentityConverter<Boolean, Boolean>())
            .put(AttributeType.DataType.DATE, new DateConverter())
            .put(AttributeType.DataType.DOUBLE, new DoubleConverter())
            .put(AttributeType.DataType.FLOAT, new FloatConverter())
            .put(AttributeType.DataType.INTEGER, new IntegerConverter())
            .put(AttributeType.DataType.LONG, new LongConverter())
            .put(AttributeType.DataType.STRING, new IdentityConverter<String, String>())
            .build();

    public static <SOURCE, TARGET> AttributeValueConverter<SOURCE, TARGET> of(AttributeType.DataType<TARGET> dataType) {
        AttributeValueConverter<?, ?> converter = converters.get(dataType);
        if (converter == null){
            throw new UnsupportedOperationException("Unsupported DataType: " + dataType.toString());
        }
        return (AttributeValueConverter<SOURCE, TARGET>) converter;
    }

    public abstract TARGET convert(SOURCE value);

    public static class IdentityConverter<SOURCE, TARGET> extends AttributeValueConverter<SOURCE, TARGET> {
        @Override
        public TARGET convert(SOURCE value) { return (TARGET) value;}
    }

    public static class DateConverter extends AttributeValueConverter<Object, LocalDateTime> {

        @Override
        public LocalDateTime convert(Object value) {
            if (value instanceof LocalDateTime){
                return (LocalDateTime) value;
            } else if (value instanceof LocalDate){
                return ((LocalDate) value).atStartOfDay();
            }
            //NB: we are not able to parse ZonedDateTime correctly so leaving that for now
            throw new ClassCastException();
        }
    }

    public static class DoubleConverter extends AttributeValueConverter<Number, Double> {
        @Override
        public Double convert(Number value) {
            return value.doubleValue();
        }
    }

    public static class FloatConverter extends AttributeValueConverter<Number, Float> {
        @Override
        public Float convert(Number value) {
            return value.floatValue();
        }
    }

    public static class IntegerConverter extends AttributeValueConverter<Number, Integer> {
        @Override
        public Integer convert(Number value) {
            if ( value.floatValue() % 1 == 0) return value.intValue();
            throw new ClassCastException();
        }
    }

    public static class LongConverter extends AttributeValueConverter<Number, Long> {
        @Override
        public Long convert(Number value) {
            if ( value.floatValue() % 1 == 0) return value.longValue();
            throw new ClassCastException();
        }
    }
}
