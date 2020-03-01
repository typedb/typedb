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

package grakn.core.kb.concept.api;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * An ontological element which categorises how Things may relate to each other.
 * A RelationType defines how Type may relate to one another.
 * They are used to model and categorise n-ary Relations.
 */
public interface RelationType extends Type {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    RelationType label(Label label);

    /**
     * Creates and returns a new Relation instance, whose direct type will be this type.
     *
     * @return a new empty relation.
     * see Relation
     */
    Relation create();
    Relation addRelationInferred();

    /**
     * Sets the supertype of the RelationType to be the RelationType specified.
     *
     * @param type The supertype of this RelationType
     * @return The RelationType itself.
     */
    RelationType sup(RelationType type);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType key(AttributeType attributeType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RelationType has(AttributeType attributeType);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieves a list of the RoleTypes that make up this RelationType.
     *
     * @return A list of the RoleTypes which make up this RelationType.
     * see Role
     */
    @CheckReturnValue
    Stream<Role> roles();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     * Sets a new Role for this RelationType.
     *
     * @param role A new role which is part of this relation.
     * @return The RelationType itself.
     * see Role
     */
    RelationType relates(Role role);

    //------------------------------------- Other ----------------------------------

    /**
     * Unrelates a Role from this RelationType
     *
     * @param role The Role to unrelate from the RelationType.
     * @return The RelationType itself.
     * see Role
     */
    RelationType unrelate(Role role);

    //---- Inherited Methods

    /**
     * Sets the RelationType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract Specifies if the concept is to be abstract (true) or not (false).
     * @return The RelationType itself.
     */
    @Override
    RelationType isAbstract(Boolean isAbstract);

    /**
     * Returns the direct supertype of this RelationType.
     *
     * @return The direct supertype of this RelationType
     */
    @Override
    @Nullable
    RelationType sup();

    /**
     * Returns a collection of supertypes of this RelationType.
     *
     * @return All the supertypes of this RelationType
     */
    @Override
    Stream<RelationType> sups();

    /**
     * Returns a collection of subtypes of this RelationType.
     *
     * @return All the sub types of this RelationType
     */
    @Override
    Stream<RelationType> subs();

    /**
     * Sets the Role which instances of this RelationType may play.
     *
     * @param role The Role which the instances of this Type are allowed to play.
     * @return The RelationType itself.
     */
    @Override
    RelationType plays(Role role);

    /**
     * Removes the ability of this RelationType to play a specific Role
     *
     * @param role The Role which the Things of this Rule should no longer be allowed to play.
     * @return The Rule itself.
     */
    @Override
    RelationType unplay(Role role);

    /**
     * Removes the ability for Things of this RelationType to have Attributes of type AttributeType
     *
     * @param attributeType the AttributeType which this RelationType can no longer have
     * @return The RelationType itself.
     */
    @Override
    RelationType unhas(AttributeType attributeType);

    /**
     * Removes AttributeType as a key to this RelationType
     *
     * @param attributeType the AttributeType which this RelationType can no longer have as a key
     * @return The RelationType itself.
     */
    @Override
    RelationType unkey(AttributeType attributeType);

    /**
     * Retrieve all the Relation instances of this RelationType
     *
     * @return All the Relation instances of this RelationType
     * see Relation
     */
    @Override
    Stream<Relation> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default RelationType asRelationType() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRelationType() {
        return true;
    }
}
