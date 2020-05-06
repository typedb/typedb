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

public interface AttributeTypeInt extends ThingTypeInt {

    @Override
    AttributeTypeInt sup();

    @Override
    Stream<? extends AttributeTypeInt> sups();

    @Override
    Stream<? extends AttributeTypeInt> subs();

    void sup(AttributeTypeInt superType);

    Class<?> valueClass();

    AttributeTypeInt asObject();

    AttributeTypeInt.Boolean asBoolean();

    AttributeTypeInt.Long asLong();

    AttributeTypeInt.Double asDouble();

    AttributeTypeInt.String asString();

    AttributeTypeInt.DateTime asDateTime();

    interface Boolean extends AttributeTypeInt {

    }

    interface Long extends AttributeTypeInt {

    }

    interface Double extends AttributeTypeInt {

    }

    interface String extends AttributeTypeInt {

    }

    interface DateTime extends AttributeTypeInt {

    }
}
