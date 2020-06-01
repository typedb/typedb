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

package hypergraph.concept.type;

import hypergraph.concept.thing.Attribute;

import java.time.LocalDateTime;
import java.util.stream.Stream;

public interface AttributeType extends ThingType {

    @Override
    default AttributeType asAttributeType() { return this; }

    @Override
    AttributeType sup();

    @Override
    Stream<? extends AttributeType> sups();

    @Override
    Stream<? extends AttributeType> subs();

    @Override
    Stream<? extends Attribute> instances();

    void sup(AttributeType superType);

    boolean isKeyable();

    Class<?> valueType();

    AttributeType asObject();

    AttributeType.Boolean asBoolean();

    AttributeType.Long asLong();

    AttributeType.Double asDouble();

    AttributeType.String asString();

    AttributeType.DateTime asDateTime();

    interface Boolean extends AttributeType {

        @Override
        AttributeType.Boolean sup();

        @Override
        Stream<? extends AttributeType.Boolean> sups();

        @Override
        Stream<? extends AttributeType.Boolean> subs();

        @Override
        Stream<? extends Attribute.Boolean> instances();

        Attribute.Boolean put(boolean value);

        Attribute.Boolean put(boolean value, boolean isInferred);

        Attribute.Boolean get(boolean value);
    }

    interface Long extends AttributeType {

        @Override
        AttributeType.Long sup();

        @Override
        Stream<? extends AttributeType.Long> sups();

        @Override
        Stream<? extends AttributeType.Long> subs();

        @Override
        Stream<? extends Attribute.Long> instances();

        Attribute.Long put(long value);

        Attribute.Long put(long value, boolean isInferred);

        Attribute.Long get(int value);
    }

    interface Double extends AttributeType {

        @Override
        AttributeType.Double sup();

        @Override
        Stream<? extends AttributeType.Double> sups();

        @Override
        Stream<? extends AttributeType.Double> subs();

        @Override
        Stream<? extends Attribute.Double> instances();

        Attribute.Double put(double value);

        Attribute.Double put(double value, boolean isInferred);

        Attribute.Double get(double value);
    }

    interface String extends AttributeType {

        @Override
        AttributeType.String sup();

        @Override
        Stream<? extends AttributeType.String> sups();

        @Override
        Stream<? extends AttributeType.String> subs();

        @Override
        Stream<? extends Attribute.String> instances();

        Attribute.String put(java.lang.String value);

        Attribute.String put(java.lang.String value, boolean isInferred);

        Attribute.String get(java.lang.String value);
    }

    interface DateTime extends AttributeType {

        @Override
        AttributeType.DateTime sup();

        @Override
        Stream<? extends AttributeType.DateTime> sups();

        @Override
        Stream<? extends AttributeType.DateTime> subs();

        @Override
        Stream<? extends Attribute.DateTime> instances();

        Attribute.DateTime put(LocalDateTime value);

        Attribute.DateTime put(LocalDateTime value, boolean isInferred);

        Attribute.DateTime get(LocalDateTime value);
    }
}
