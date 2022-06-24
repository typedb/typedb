/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.concept.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.ThingType;

public interface Attribute extends Thing {

    /**
     * Get the immediate {@code AttributeType} in which this this {@code Attribute} is an instance of.
     *
     * @return the {@code AttributeType} of this {@code Attribute}
     */
    @Override
    AttributeType getType();

    /**
     * Get a iterator of all {@code Thing} instances that own this {@code Attribute}.
     *
     * @return iterator of all {@code Thing} instances that own this {@code Attribute}
     */
    FunctionalIterator<? extends Thing> getOwners();

    FunctionalIterator<? extends Thing> getOwners(ThingType ownerType);

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isDateTime();

    Attribute.Boolean asBoolean();

    Attribute.Long asLong();

    Attribute.Double asDouble();

    Attribute.String asString();

    Attribute.DateTime asDateTime();

    interface Boolean extends Attribute {

        java.lang.Boolean getValue();
    }

    interface Long extends Attribute {

        java.lang.Long getValue();
    }

    interface Double extends Attribute {

        java.lang.Double getValue();
    }

    interface String extends Attribute {

        java.lang.String getValue();
    }

    interface DateTime extends Attribute {

        java.time.LocalDateTime getValue();
    }
}
