/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.thing;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.ThingType;

public interface Attribute extends Thing, Concept.Readable {

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
