/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.concept;

import ai.grakn.exception.GraknTxOperationException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * <p>
 *     An ontological element which categorises how {@link Thing}s may relate to each other.
 * </p>
 *
 * <p>
 *     A {@link RelationshipType} defines how {@link Type} may relate to one another.
 *     They are used to model and categorise n-ary {@link Relationship}s.
 * </p>
 *
 * @author fppt
 *
 */
public interface RelationshipType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    RelationshipType setLabel(Label label);

    /**
     * Creates and returns a new {@link Relationship} instance, whose direct type will be this type.
     * @see Relationship
     *
     * @return a new empty relation.
     *
     * @throws GraknTxOperationException if this is a meta type
     */
    Relationship addRelationship();

    /**
     * Sets the supertype of the {@link RelationshipType} to be the {@link RelationshipType} specified.
     *
     * @param type The supertype of this {@link RelationshipType}
     * @return  The {@link RelationshipType} itself.
     */
    RelationshipType sup(RelationshipType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this {@link RelationshipType}
     * @return The {@link RelationshipType} itself
     */
    RelationshipType sub(RelationshipType type);

    /**
     * Creates a {@link RelationshipType} which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationshipType key(AttributeType attributeType);

    /**
     * Creates a {@link RelationshipType} which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationshipType attribute(AttributeType attributeType);

    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves a list of the RoleTypes that make up this {@link RelationshipType}.
     * @see Role
     *
     * @return A list of the RoleTypes which make up this {@link RelationshipType}.
     */
    @CheckReturnValue
    Stream<Role> relates();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     * Sets a new Role for this {@link RelationshipType}.
     * @see Role
     *
     * @param role A new role which is part of this relationship.
     * @return The {@link RelationshipType} itself.
     */
    RelationshipType relates(Role role);

    //------------------------------------- Other ----------------------------------

    /**
     * Delete a Role from this {@link RelationshipType}
     * @see Role
     *
     * @param role The Role to delete from the {@link RelationshipType}.
     * @return The {@link RelationshipType} itself.
     */
    RelationshipType deleteRelates(Role role);

    //---- Inherited Methods
    /**
     * Sets the {@link RelationshipType} to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The {@link RelationshipType} itself.
     */
    @Override
    RelationshipType setAbstract(Boolean isAbstract);

    /**
     * Returns the direct supertype of this {@link RelationshipType}.
     * @return The direct supertype of this {@link RelationshipType}
     */
    @Override
    @Nullable
    RelationshipType sup();

    /**
     * Returns a collection of supertypes of this {@link RelationshipType}.
     * @return All the supertypes of this {@link RelationshipType}
     */
    @Override
    Stream<RelationshipType> sups();

    /**
     * Returns a collection of subtypes of this {@link RelationshipType}.
     *
     * @return All the sub types of this {@link RelationshipType}
     */
    @Override
    Stream<RelationshipType> subs();

    /**
     * Sets the Role which instances of this {@link RelationshipType} may play.
     *
     * @param role The Role which the instances of this Type are allowed to play.
     * @return  The {@link RelationshipType} itself.
     */
    @Override
    RelationshipType plays(Role role);

    /**
     * Removes the ability of this {@link RelationshipType} to play a specific {@link Role}
     *
     * @param role The {@link Role} which the {@link Thing}s of this {@link Rule} should no longer be allowed to play.
     * @return The {@link Rule} itself.
     */
    @Override
    RelationshipType deletePlays(Role role);

    /**
     * Removes the ability for {@link Thing}s of this {@link RelationshipType} to have {@link Attribute}s of type {@link AttributeType}
     *
     * @param attributeType the {@link AttributeType} which this {@link RelationshipType} can no longer have
     * @return The {@link RelationshipType} itself.
     */
    @Override
    RelationshipType deleteAttribute(AttributeType attributeType);

    /**
     * Removes {@link AttributeType} as a key to this {@link RelationshipType}
     *
     * @param attributeType the {@link AttributeType} which this {@link RelationshipType} can no longer have as a key
     * @return The {@link RelationshipType} itself.
     */
    @Override
    RelationshipType deleteKey(AttributeType attributeType);

    /**
     * Retrieve all the {@link Relationship} instances of this {@link RelationshipType}
     * @see Relationship
     *
     * @return All the {@link Relationship} instances of this {@link RelationshipType}
     */
    @Override
    Stream<Relationship> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default RelationshipType asRelationshipType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelationshipType(){
        return true;
    }
}
