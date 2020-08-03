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

import com.google.common.annotations.VisibleForTesting;
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

    @VisibleForTesting
    public static Map<AttributeType.ValueType<?>, AttributeValueConverter<?>> converters = ImmutableMap.<AttributeType.ValueType<?>, AttributeValueConverter<?>>builder()
            .put(AttributeType.ValueType.BOOLEAN, new BooleanConverter())
            .put(AttributeType.ValueType.DATETIME, new DateConverter())
            .put(AttributeType.ValueType.DOUBLE, new DoubleConverter())
            .put(AttributeType.ValueType.LONG, new LongConverter())
            .put(AttributeType.ValueType.STRING, new StringConverter())
            .build();

    /**
     * Try to convert an attribute value to a desired target type, throwing an exception if the conversion fails.
     * This is strict - we require, in most cases, that the type of value provided matches the
     * desired value type exactly
     */
    public static <T> T tryConvertForWrite(AttributeType<T> type, Object value) {
        try {
            final AttributeType.ValueType<T> valueType = type.valueType();
            AttributeValueConverter<T> converter = (AttributeValueConverter<T>) converters.get(valueType);
            if (converter == null) {
                throw new UnsupportedOperationException("Unsupported ValueType: " + valueType);
            }
            return converter.convertForWrite(value);
        } catch (ClassCastException e) {
            throw GraknConceptException.invalidAttributeValueWrite(type, value);
        }
    }

    /**
     * Try to convert an attribute value to a desired target type, throwing an exception if the conversion fails.
     * This is slightly more lax than write conversions, as we allow doubles to retrieve longs if decimals are compatible
     */
    public static <T> T tryConvertForRead(AttributeType<T> type, Object value) {
        try {
            final AttributeType.ValueType<T> valueType = type.valueType();
            AttributeValueConverter<T> converter = (AttributeValueConverter<T>) converters.get(valueType);
            if (converter == null) {
                throw new UnsupportedOperationException("Unsupported ValueType: " + valueType);
            }
            return converter.convertForRead(value);
        } catch (ClassCastException e) {
            throw GraknConceptException.invalidAttributeValueRead(type, value);
        }
    }

    // strict conversion for writes
    abstract TARGET convertForWrite(Object value);
    // slightly laxer conversion for reads
    abstract TARGET convertForRead(Object value);

    public static class BooleanConverter extends AttributeValueConverter<Boolean> {
        @Override
        Boolean convertForWrite(Object value) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new ClassCastException();
        }

        @Override
        Boolean convertForRead(Object value) {
            return convertForWrite(value);
        }
    }

    public static class StringConverter extends AttributeValueConverter<String> {
        @Override
        String convertForWrite(Object value) {
            if (value instanceof String) {
                return value.toString();
            }
            throw new ClassCastException();
        }

        @Override
        String convertForRead(Object value) {
            return convertForWrite(value);
        }
    }

    public static class DateConverter extends AttributeValueConverter<LocalDateTime> {

        @Override
        LocalDateTime convertForWrite(Object value) {
            if (value instanceof LocalDateTime) {
                return (LocalDateTime) value;
            } else if (value instanceof LocalDate) {
                return ((LocalDate) value).atStartOfDay();
            }
            //NB: we are not able to parse ZonedDateTime correctly so leaving that for now
            throw new ClassCastException();
        }

        @Override
        LocalDateTime convertForRead(Object value) {
            return convertForWrite(value);
        }
    }

    public static class DoubleConverter extends AttributeValueConverter<Double> {
        @Override
        Double convertForWrite(Object value) {
            if (value instanceof Long) {
                return ((Long) value).doubleValue();
            } else if (value instanceof Integer) {
                return ((Integer) value).doubleValue();
            } else if (value instanceof Double) {
                return (Double) value;
            }
            throw new ClassCastException();
        }

        @Override
        Double convertForRead(Object value) {
            return convertForWrite(value);
        }
    }

    public static class LongConverter extends AttributeValueConverter<Long> {
        @Override
        Long convertForWrite(Object value) {
            if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof Integer) {
                return Long.valueOf((Integer) value);
            }
            throw new ClassCastException();
        }

        /*
        At read time, we do allow converting 2.0 to 2 and retriving it as such
        */
        @Override
        Long convertForRead(Object value) {
            if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof Integer) {
                return Long.valueOf((Integer) value);
            } else if (value instanceof Double) {
                if (((Double)value) % 1 == 0) {
                    return ((Double)value).longValue();
                }
            }
            throw new ClassCastException();
        }
    }
}
