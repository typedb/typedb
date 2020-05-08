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

import java.util.stream.Stream;

public interface AttributeType extends ThingType {

    @Override
    AttributeType sup();

    @Override
    Stream<? extends AttributeType> sups();

    @Override
    Stream<? extends AttributeType> subs();

    void sup(AttributeType superType);

    Class<?> valueClass();

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

    }

    interface Long extends AttributeType {

        @Override
        AttributeType.Long sup();

        @Override
        Stream<? extends AttributeType.Long> sups();

        @Override
        Stream<? extends AttributeType.Long> subs();
    }

    interface Double extends AttributeType {

        @Override
        AttributeType.Double sup();

        @Override
        Stream<? extends AttributeType.Double> sups();

        @Override
        Stream<? extends AttributeType.Double> subs();
    }

    interface String extends AttributeType {

        @Override
        AttributeType.String sup();

        @Override
        Stream<? extends AttributeType.String> sups();

        @Override
        Stream<? extends AttributeType.String> subs();
    }

    interface DateTime extends AttributeType {

        @Override
        AttributeType.DateTime sup();

        @Override
        Stream<? extends AttributeType.DateTime> sups();

        @Override
        Stream<? extends AttributeType.DateTime> subs();
    }
}
