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

import grakn.core.graql.concept.AttributeType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public abstract class DataValue<PRESENTED, PERSISTED> {

    public static final DataValue<java.lang.Boolean, java.lang.Boolean> BOOLEAN = new DataValue.Boolean();
    public static final DataValue<java.lang.Double, java.lang.Double> DOUBLE = new DataValue.Double();
    public static final DataValue<java.lang.Float, java.lang.Float> FLOAT = new DataValue.Float();
    public static final DataValue<java.lang.Integer, java.lang.Integer> INTEGER = new DataValue.Integer();
    public static final DataValue<java.lang.Long, java.lang.Long> LONG = new DataValue.Long();
    public static final DataValue<java.lang.String, java.lang.String> STRING = new DataValue.String();
    public static final DataValue<java.time.LocalDateTime, java.lang.Long> DATE = new DataValue.Date();

    private AttributeType.DataType dataType;

    DataValue(AttributeType.DataType dataType) {
        this.dataType = dataType;
    }

    public AttributeType.DataType getDataType() {
        return dataType;
    }

    public abstract PERSISTED persisted(PRESENTED value);

    public abstract PRESENTED presented(PERSISTED value);

    // TODO: This method should not be needed if all usage of this class is
    //       accessed via the constant properties defined above.
    public static <PRESENTED> DataValue<PRESENTED, ?> of(AttributeType.DataType<PRESENTED> dataType) {
        if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Boolean();

        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Date();

        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Double();

        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Double();

        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Integer();

        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return (DataValue<PRESENTED, ?>) new DataValue.Long();

        } else if (dataType.equals(AttributeType.DataType.STRING)) {
            return (DataValue<PRESENTED, ?>) new DataValue.String();

        } else {
            throw new UnsupportedOperationException("Unsupported DataType: " + dataType.toString());
        }
    }

    private static abstract class Default<VALUE> extends DataValue<VALUE, VALUE> {

        Default(AttributeType.DataType dataType) {
            super(dataType);
        }

        @Override
        public VALUE persisted(VALUE value) {
            return value;
        }

        @Override
        public VALUE presented(VALUE value) {
            return value;
        }
    }

    public static class Boolean extends Default<java.lang.Boolean> {
        Boolean() {
            super(AttributeType.DataType.BOOLEAN);
        }
    }

    public static class Double extends Default<java.lang.Double> {
        Double() {
            super(AttributeType.DataType.DOUBLE);
        }
    }

    public static class Float extends Default<java.lang.Float> {
        Float() {
            super(AttributeType.DataType.FLOAT);
        }
    }

    public static class Integer extends Default<java.lang.Integer> {
        Integer() {
            super(AttributeType.DataType.INTEGER);
        }
    }

    public static class Long extends Default<java.lang.Long> {
        Long() {
            super(AttributeType.DataType.LONG);
        }
    }

    public static class String extends Default<java.lang.String> {
        String() {
            super(AttributeType.DataType.STRING);
        }
    }

    public static class Date extends DataValue<java.time.LocalDateTime, java.lang.Long> {

        Date() {
            super(AttributeType.DataType.DATE);
        }

        @Override
        public java.lang.Long persisted(LocalDateTime value) {
            return value.atZone(ZoneId.of("Z")).toInstant().toEpochMilli();
        }

        @Override
        public LocalDateTime presented(java.lang.Long value) {
            if (value == null) return null;
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.of("Z"));
        }
    }
}

