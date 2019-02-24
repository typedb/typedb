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

public abstract class Serialise<DESERIALISED, SERIALISED> {

    public static final Serialise<java.lang.Boolean, java.lang.Boolean> BOOLEAN = new Default<>();
    public static final Serialise<java.lang.Double, java.lang.Double> DOUBLE = new Default<>();
    public static final Serialise<java.lang.Float, java.lang.Float> FLOAT = new Default<>();
    public static final Serialise<java.lang.Integer, java.lang.Integer> INTEGER = new Default<>();
    public static final Serialise<java.lang.Long, java.lang.Long> LONG = new Default<>();
    public static final Serialise<java.lang.String, java.lang.String> STRING = new Default<>();
    public static final Serialise<LocalDateTime, java.lang.Long> DATE = new Serialise.Date();

    Serialise() {}

    public abstract SERIALISED serialised(DESERIALISED value);

    public abstract DESERIALISED deserialised(SERIALISED value);

    // TODO: This method should not be needed if all usage of this class is
    //       accessed via the constant properties defined above.
    public static <DESERIALISED> Serialise<DESERIALISED, ?> of(AttributeType.DataType<DESERIALISED> dataType) {
        if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.Boolean>();

        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return (Serialise<DESERIALISED, ?>) new Serialise.Date();

        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.Double>();

        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.Double>();

        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.Integer>();

        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.Long>();

        } else if (dataType.equals(AttributeType.DataType.STRING)) {
            return (Serialise<DESERIALISED, ?>) new Default<java.lang.String>();

        } else {
            throw new UnsupportedOperationException("Unsupported DataType: " + dataType.toString());
        }
    }

    public static class Default<VALUE> extends Serialise<VALUE, VALUE> {

        @Override
        public VALUE serialised(VALUE value) {
            return value;
        }

        @Override
        public VALUE deserialised(VALUE value) {
            return value;
        }
    }

    public static class Date extends Serialise<LocalDateTime, java.lang.Long> {

        Date() {
            super();
        }

        @Override
        public java.lang.Long serialised(LocalDateTime value) {
            return value.atZone(ZoneId.of("Z")).toInstant().toEpochMilli();
        }

        @Override
        public LocalDateTime deserialised(java.lang.Long value) {
            if (value == null) return null;
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.of("Z"));
        }
    }
}

