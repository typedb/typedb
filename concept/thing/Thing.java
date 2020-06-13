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

package hypergraph.concept.thing;

import hypergraph.concept.Concept;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.ThingType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public interface Thing extends Concept {

    /**
     * Cast the {@code Concept} down to {@code Thing}.
     *
     * @return this {@code Thing}
     */
    @Override
    default Thing asThing() { return this; }

    /**
     * Returns the {@code IID} of this {@code Thing} has hexadecimal string.
     *
     * @return the {@code IID} of this {@code Thing} has hexadecimal string
     */
    @Override
    String iid();

    /**
     * Get the immediate {@code ThingType} in which this this {@code Thing} is
     * an instance of.
     *
     * @return the {@code ThingType} of this {@code Thing}
     */
    ThingType type();

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
     * @return this {@code Thing} for further manipulation
     */
    Thing has(Attribute attribute);

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
    void unhas(Attribute attribute);

    /**
     * Get all {@code Attribute} instances that are keys to this {@code Thing}.
     *
     * @return a stream of {@code Attribute} instances that are keys of this {@code Thing}
     */
    default Stream<? extends Attribute> keys() {
        return keys(Collections.emptyList());
    }

    /**
     * Get all {@code Attribute} instances that are keys to this {@code Thing}
     * filtered by an {@code AttributeType} type.
     *
     * Get all {@code Attribute} instances that are keys to this {@code Thing}
     * filtered by an {@code AttributeType} type. Although we are providing
     * one {@code AttributeType} to this method, it is possible that it is a
     * supertype of multiple {@code AttributeType} keys to this {@code Thing}.
     *
     * @return a stream of {@code Attribute} instances that are keys of this {@code Thing}
     */
    default Stream<? extends Attribute> keys(AttributeType attributeType) {
        return keys(Collections.singletonList(attributeType));
    }

    /**
     * Get all {@code Attribute} instances that are keys to this {@code Thing}
     * filtered by their {@code AttributeType} types.
     *
     * @return a stream of {@code Attribute} instances that are keys of this {@code Thing}
     */
    Stream<? extends Attribute> keys(List<AttributeType> attributeTypes);

    /**
     * Get all {@code Attribute} instances owned by this {@code Thing}.
     *
     * @return a stream of {@code Attribute} instances owned by this {@code Thing}
     */
    default Stream<? extends Attribute> attributes() {
        return attributes(Collections.emptyList());
    }

    /**
     * Get all {@code Attribute} instances owned by this {@code Thing} filtered
     * by an {@code AttributeType} type.
     *
     * @return a stream of {@code Attribute} instances owned by this {@code Thing}
     */
    default Stream<? extends Attribute> attributes(AttributeType attributeTypes) {
        return attributes(Collections.singletonList(attributeTypes));
    }

    /**
     * Get all {@code Attribute} instances owned by this {@code Thing} filtered
     * by their {@code AttributeType} types.
     *
     * The {@code Attribute} instances
     * owned by this {@code Thing} include {@code Attribute} instances that
     * serve as key {@code Attributes}.
     *
     * @return a stream of {@code Attribute} instances owned by this {@code Thing}
     */
    Stream<? extends Attribute> attributes(List<AttributeType> attributeTypes);

    /**
     * Get all {@code RoleType} types that this {@code Thing} plays in a {@code Relation}.
     *
     * @return a stream of {@code RoleType} types that this {@code Thing} plays.
     */
    Stream<? extends RoleType> roles();

    /**
     * Get all {@code Relation} instances that this {@code Thing} is plays in.
     *
     * @param roleTypes that this {@code Thing} can play
     * @return a stream of {@code Relation} that this {@code Thing} plays in
     */
    Stream<? extends Relation> relations(List<RoleType> roleTypes);

    /**
     * Deletes this {@code Thing} from the system.
     */
    void delete();
}
