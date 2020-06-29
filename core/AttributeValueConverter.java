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
import grakn.core.kb.concept.api.GraknConceptException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Convert Attribute Values obtained from Graql and stored in Properties, and convert
 * them to native java data instances
 */
public abstract class AttributeValueConverter<SOURCE, TARGET>{

    private static Map<AttributeType.ValueType<?>, AttributeValueConverter<?, ?>> converters = ImmutableMap.<AttributeType.ValueType<?>, AttributeValueConverter<?, ?>>builder()
            .put(AttributeType.ValueType.BOOLEAN, new IdentityConverter<Boolean, Boolean>())
            .put(AttributeType.ValueType.DATETIME, new DateConverter())
            .put(AttributeType.ValueType.DOUBLE, new DoubleConverter())
            .put(AttributeType.ValueType.FLOAT, new FloatConverter())
            .put(AttributeType.ValueType.INTEGER, new IntegerConverter())
            .put(AttributeType.ValueType.LONG, new LongConverter())
            .put(AttributeType.ValueType.STRING, new IdentityConverter<String, String>())
            .build();

    /**
     * Try to convert an attribute value to a desired target type, throwing an exception if the conversion fails.
     * @param type The attribute type
     * @param value The attribute value
     * @param <S> The source type
     * @param <T> The target type
     * @return The converted value
     */
    public static <S, T> T tryConvert(AttributeType<T> type, S value) {
        try {
            final AttributeType.ValueType<T> valueType = type.valueType();
            AttributeValueConverter<S, T> converter = (AttributeValueConverter<S, T>) converters.get(valueType);
            if (converter == null) {
                throw new UnsupportedOperationException("Unsupported ValueType: " + valueType.toString());
            }
            return converter.convert(value);
        } catch (ClassCastException e) {
            throw GraknConceptException.invalidAttributeValue(type, value);
        }
    }

    abstract TARGET convert(SOURCE value);

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
