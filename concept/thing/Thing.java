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

import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.Type;

import java.util.stream.Stream;

public interface Thing {

    /**
     * Get the immediate {@code Type} in which this this {@code Thing} is an instance of.
     *
     * @return the {@code Type} of this {@code Thing}
     */
    Type type();

    /**
     * Get a boolean representing whether this {@code Thing} is inferred.
     *
     * @return true if this {@code Thing} is inferred, else false
     */
    boolean isInferred();

    /**
     * Set an {@code Attribute} to be a key this {@code Thing}.
     *
     * A {@code Thing} must have one and only one {@code Attribute} with the same {@code AttributeType},
     * and that {@code Attribute} cannot be owned by another {@code Thing} of the same {@code Type} as this one.
     *
     * @param   attribute   that will be a key to this {@code Thing}
     * @return              this {@code Type} for further manipulation
     */
    Type key(Attribute attribute);

    /**
     * Remove an {@code Attribute} from being a key to this {@code Thing}.
     *
     * Once the key {@code Attribute} of this {@code Thing} has been removed, the {@code Thing}
     * needs to have a new {@code Attribute} assigned as its key before being committed.
     * Otherwise, this {@code Thing} will fail validation as it no longer as a key.
     * The {@code Attribute} that has been removed can also be owned by a new {@code Thing}.
     *
     * @param   attribute   that will no longer be a key to this {@code Thing}
     */
    void unkey(Attribute attribute);

    /**
     * Get all {@code Attribute} that are keys to this {@code Thing}.
     *
     * @return a stream of {@code Attribute} instances that are keys of this {@code Thing}
     */
    Stream<? extends Attribute> keys();

    /**
     * Set an {@code Attribute} to be owned by this {@code Thing}.
     *
     * @param   attribute   that will be owned by this {@code Thing}
     * @return              this {@code Thing} for further manipulation
     */
    Type has(Attribute attribute);

    /**
     * Remove an {@code Attribute} from being owned by this {@code Thing}.
     *
     * @param   attribute   that will no longer be owned by this {@code Thing}
     */
    void unhas(Attribute attribute);

    /**
     * Get all {@code Attribute} instances that are owned by this {@code Thing}.
     *
     * The {@code Attribute} instances owned by this {@code Thing} include those that are
     * merely owned by this {@code Thing}, as well as those that serve as key {@code Attributes}.
     *
     * @return a stream of {@code Attribute} instances owned by this {@code Thing}
     */
    Stream<? extends Attribute> attributes();

    /**
     * Get all {@code RoleType} types that this {@code Thing} plays in a {@code Relation}.
     *
     * @return a stream of {@code RoleType} types that this {@code Thing} plays.
     */
    Stream<? extends RoleType> roles();

    /**
     * Get all {@code Relation} instances that this {@code Thing} is plays in.
     *
     * @param   roleTypes   that this {@code Thing} can play
     * @return              a stream of {@code Relation} that this {@code Thing} plays in
     */
    Stream<? extends Relation> relations(RoleType... roleTypes);

    /**
     * Get all concepts that this {@code Thing} depends on in order to exist.
     *
     * For example, if this were to be an inferred concept, and you need to material this {@code Thing},
     * what are the other concepts that needs to exist for this {@code Thing} to be able to refer to?
     *
     * @return a stream {@code Thing} concepts that this {@code Thing} depends on
     */
    Stream<? extends Thing> dependency();

}
