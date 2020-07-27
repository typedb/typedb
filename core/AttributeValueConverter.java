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
public abstract class AttributeValueConverter<TARGET> {

    private static Map<AttributeType.ValueType<?>, AttributeValueConverter<?>> converters = ImmutableMap.<AttributeType.ValueType<?>, AttributeValueConverter<?>>builder()
            .put(AttributeType.ValueType.BOOLEAN, new BooleanConverter())
            .put(AttributeType.ValueType.DATETIME, new DateConverter())
            .put(AttributeType.ValueType.DOUBLE, new DoubleConverter())
            .put(AttributeType.ValueType.LONG, new LongConverter())
            .put(AttributeType.ValueType.STRING, new StringConverter())
            .build();

    public static <TARGET> AttributeValueConverter<TARGET> of(AttributeType.ValueType<TARGET> valueType) {
        AttributeValueConverter<?> converter = converters.get(valueType);
        if (converter == null){
            throw new UnsupportedOperationException("Unsupported ValueType: " + valueType.toString());
        }
        return (AttributeValueConverter<TARGET>) converter;
    }

    public abstract TARGET convert(Object value);

    public static class BooleanConverter extends AttributeValueConverter<Boolean> {
        @Override
        public Boolean convert(Object value) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new ClassCastException();
        }
    }

    public static class StringConverter extends AttributeValueConverter<String> {
        @Override
        public String convert(Object value) {
            if (value instanceof String) {
                return value.toString();
            }
            throw new ClassCastException();
        }
    }

    public static class DateConverter extends AttributeValueConverter<LocalDateTime> {

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

    public static class DoubleConverter extends AttributeValueConverter<Double> {
        @Override
        public Double convert(Object value) {
            if (value instanceof Long) {
                return ((Long)value).doubleValue();
            } else if (value instanceof Double){
                return (Double) value;
            }
            throw new ClassCastException();
        }
    }

    public static class LongConverter extends AttributeValueConverter<Long> {
        @Override
        public Long convert(Object value) {
            if (value instanceof Long) {
                return (Long) value;
            }
            throw new ClassCastException();
        }
    }
}
