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

package grakn.core.graql.concept;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * An ontological element which categorises how {@link Thing}s may relate to each other.
 * A {@link RelationType} defines how {@link Type} may relate to one another.
 * They are used to model and categorise n-ary {@link Relation}s.
 */
public interface RelationType extends Type {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     *
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    RelationType label(Label label);

    /**
     * Creates and returns a new {@link Relation} instance, whose direct type will be this type.
     *
     * @return a new empty relation.
     * @see Relation
     */
    Relation create();

    /**
     * Sets the supertype of the {@link RelationType} to be the {@link RelationType} specified.
     *
     * @param type The supertype of this {@link RelationType}
     * @return The {@link RelationType} itself.
     */
    RelationType sup(RelationType type);

    /**
     * Creates a {@link RelationType} which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType key(AttributeType attributeType);

    /**
     * Creates a {@link RelationType} which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType has(AttributeType attributeType);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieves a list of the RoleTypes that make up this {@link RelationType}.
     *
     * @return A list of the RoleTypes which make up this {@link RelationType}.
     * @see Role
     */
    @CheckReturnValue
    Stream<Role> roles();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     * Sets a new Role for this {@link RelationType}.
     *
     * @param role A new role which is part of this relationship.
     * @return The {@link RelationType} itself.
     * @see Role
     */
    RelationType relates(Role role);

    //------------------------------------- Other ----------------------------------

    /**
     * Unrelates a Role from this {@link RelationType}
     *
     * @param role The Role to unrelate from the {@link RelationType}.
     * @return The {@link RelationType} itself.
     * @see Role
     */
    RelationType unrelate(Role role);

    //---- Inherited Methods

    /**
     * Sets the {@link RelationType} to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract Specifies if the concept is to be abstract (true) or not (false).
     * @return The {@link RelationType} itself.
     */
    @Override
    RelationType isAbstract(Boolean isAbstract);

    /**
     * Returns the direct supertype of this {@link RelationType}.
     *
     * @return The direct supertype of this {@link RelationType}
     */
    @Override
    @Nullable
    RelationType sup();

    /**
     * Returns a collection of supertypes of this {@link RelationType}.
     *
     * @return All the supertypes of this {@link RelationType}
     */
    @Override
    Stream<RelationType> sups();

    /**
     * Returns a collection of subtypes of this {@link RelationType}.
     *
     * @return All the sub types of this {@link RelationType}
     */
    @Override
    Stream<RelationType> subs();

    /**
     * Sets the Role which instances of this {@link RelationType} may play.
     *
     * @param role The Role which the instances of this Type are allowed to play.
     * @return The {@link RelationType} itself.
     */
    @Override
    RelationType plays(Role role);

    /**
     * Removes the ability of this {@link RelationType} to play a specific {@link Role}
     *
     * @param role The {@link Role} which the {@link Thing}s of this {@link Rule} should no longer be allowed to play.
     * @return The {@link Rule} itself.
     */
    @Override
    RelationType unplay(Role role);

    /**
     * Removes the ability for {@link Thing}s of this {@link RelationType} to have {@link Attribute}s of type {@link AttributeType}
     *
     * @param attributeType the {@link AttributeType} which this {@link RelationType} can no longer have
     * @return The {@link RelationType} itself.
     */
    @Override
    RelationType unhas(AttributeType attributeType);

    /**
     * Removes {@link AttributeType} as a key to this {@link RelationType}
     *
     * @param attributeType the {@link AttributeType} which this {@link RelationType} can no longer have as a key
     * @return The {@link RelationType} itself.
     */
    @Override
    RelationType unkey(AttributeType attributeType);

    /**
     * Retrieve all the {@link Relation} instances of this {@link RelationType}
     *
     * @return All the {@link Relation} instances of this {@link RelationType}
     * @see Relation
     */
    @Override
    Stream<Relation> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default RelationType asRelationshipType() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelationshipType() {
        return true;
    }
}
