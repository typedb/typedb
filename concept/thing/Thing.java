/*
 * Copyright (C) 2021 Vaticle
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
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.AttributeType;

public interface Thing extends Concept {

    /**
     * Returns the {@code IID} of this {@code Thing} as a byte array.
     *
     * @return the {@code IID} of this {@code Thing} as a byte array
     */
    ByteArray getIID();


    /**
     * Returns the {@code IID} of this {@code Thing} as hexadecimal string.
     *
     * @return the {@code IID} of this {@code Thing} as hexadecimal string
     */
    String getIIDForPrinting();

    /**
     * Get the immediate {@code ThingType} in which this this {@code Thing} is
     * an instance of.
     *
     * @return the {@code ThingType} of this {@code Thing}
     */
    ThingType getType();

    /**
     * Indicates whether this {@code Thing} is inferred.
     *
     * @return true if this {@code Thing} is inferred, else false
     */
    boolean isInferred();

    /**
     * Set an {@code Attribute} to be owned by this {@code Thing}.
     *
     * If the {@code Attribute} is an instance of an {@code AttributeType} that
     * is a key to the {@code ThingType} of this {@code Thing}, then this
     * {@code Thing} must have one and only one {@code Attribute} with the same
     * {@code AttributeType}, and that {@code Attribute} cannot be owned by
     * another {@code Thing} of the same {@code ThingType} as this one.
     *
     * @param attribute that will be owned by this {@code Thing}
     */
    void setHas(Attribute attribute);

    void setHas(Attribute attribute, boolean isInferred);

    /**
     * Remove an {@code Attribute} from being owned by this {@code Thing}.
     *
     * If the {@code Attribute} is an instance of an {@code AttributeType} that
     * is a key to the {@code ThingType} of this {@code Thing}, then once the
     * key {@code Attribute} of this {@code Thing} has been removed, the
     * {@code Thing} needs to have a new {@code Attribute} assigned as its key
     * before being committed. Otherwise, this {@code Thing} will fail
     * validation as it no longer as a key. The {@code Attribute} that has been
     * removed can also be owned by a new {@code Thing}.
     *
     * @param attribute that will no longer be owned by this {@code Thing}
     */
    void unsetHas(Attribute attribute);

    /**
     * Get all {@code Attribute} instances owned by this {@code Thing}.
     *
     * @return an iterator of {@code Attribute} instances owned by this {@code Thing}
     */
    FunctionalIterator<? extends Attribute> getHas(boolean onlyKey);

    FunctionalIterator<? extends Attribute.Boolean> getHas(AttributeType.Boolean attributeType);

    FunctionalIterator<? extends Attribute.Long> getHas(AttributeType.Long attributeType);

    FunctionalIterator<? extends Attribute.Double> getHas(AttributeType.Double attributeType);

    FunctionalIterator<? extends Attribute.String> getHas(AttributeType.String attributeType);

    FunctionalIterator<? extends Attribute.DateTime> getHas(AttributeType.DateTime attributeType);

    /**
     * Get all {@code Attribute} instances owned by this {@code Thing} filtered
     * by their {@code AttributeType} types. If no types are filtered, all types are retrieved.
     *
     * The {@code Attribute} instances
     * owned by this {@code Thing} include {@code Attribute} instances that
     * serve as key {@code Attributes}.
     *
     * @return an iterator of {@code Attribute} instances owned by this {@code Thing}
     */
    FunctionalIterator<? extends Attribute> getHas(AttributeType... attributeType);

    /**
     * Check whether a Has edge to a given attribute instance exists, and that edge is inferred
     *
     * @param attribute
     * @return
     */
    boolean hasInferred(Attribute attribute);

    /**
     * Get all {@code RoleType} types this {@code Thing} is playing in a {@code Relation}.
     *
     * @return an iterator stream of {@code RoleType} types that this {@code Thing} plays.
     */
    FunctionalIterator<? extends RoleType> getPlaying();

    /**
     * Get all {@code Relation} instances that this {@code Thing} is playing any of the specified roles in.
     * If no roles are specified, all Relations are retrieved regardless of role.
     *
     * @param roleTypes The role types that this {@code Thing} can play
     * @return an iterator of {@code Relation} that this {@code Thing} plays a specified role in
     */
    FunctionalIterator<? extends Relation> getRelations(String roleType, String... roleTypes);

    /**
     * Get all {@code Relation} instances that this {@code Thing} is playing any of the specified roles in.
     * If no roles are specified, all Relations are retrieved regardless of role.
     *
     * @param roleTypes The role types that this {@code Thing} can play
     * @return an iterator of {@code Relation} that this {@code Thing} plays a specified role in
     */
    FunctionalIterator<? extends Relation> getRelations(RoleType... roleTypes);

    /**
     * Returns true if this {@code Thing} has been deleted.
     *
     * @return true if this {@code Thing} has been deleted
     */
    boolean isDeleted();

    /**
     * Validates and throws an exception if there is any violation.
     */
    void validate();

}
