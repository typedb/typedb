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
 * SchemaConcept used to represent categories.
 * An ontological element which represents categories instances can fall within.
 * Any instance of a Entity Type is called an Entity.
 */
public interface EntityType extends Type {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    EntityType label(Label label);

    /**
     * Sets the EntityType to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract Specifies if the EntityType is to be abstract (true) or not (false).
     * @return The EntityType itself
     */
    @Override
    EntityType isAbstract(Boolean isAbstract);

    /**
     * Sets the direct supertype of the EntityType to be the EntityType specified.
     *
     * @param type The supertype of this EntityType
     * @return The EntityType itself
     */
    EntityType sup(EntityType type);

    /**
     * Sets the Role which instances of this EntityType may play.
     *
     * @param role The Role Type which the instances of this EntityType are allowed to play.
     * @return The EntityType itself
     */
    @Override
    EntityType plays(Role role);

    /**
     * Removes the ability of this EntityType to play a specific Role
     *
     * @param role The Role which the Things of this EntityType should no longer be allowed to play.
     * @return The EntityType itself.
     */
    @Override
    EntityType unplay(Role role);

    /**
     * Removes the ability for Things of this EntityType to have Attributes of type AttributeType
     *
     * @param attributeType the AttributeType which this EntityType can no longer have
     * @return The EntityType itself.
     */
    @Override
    EntityType unhas(AttributeType attributeType);

    /**
     * Removes AttributeType as a key to this EntityType
     *
     * @param attributeType the AttributeType which this EntityType can no longer have as a key
     * @return The EntityType itself.
     */
    @Override
    EntityType unkey(AttributeType attributeType);

    /**
     * Creates and returns a new Entity instance, whose direct type will be this type.
     *
     * @return a new empty entity.
     * see Entity
     */
    Entity create();


    Entity addEntityInferred();

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    EntityType key(AttributeType attributeType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param attributeType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    EntityType has(AttributeType attributeType);

    //------------------------------------- Accessors ----------------------------------

    /**
     * Returns the supertype of this EntityType.
     *
     * @return The supertype of this EntityType
     */
    @Override
    @Nullable
    EntityType sup();

    /**
     * Returns a collection of supertypes of this EntityType.
     *
     * @return All the super classes of this EntityType
     */
    @Override
    Stream<? extends EntityType> sups();

    /**
     * Returns a collection of subtypes of this EntityType.
     *
     * @return All the sub classes of this EntityType
     */
    @Override
    Stream<? extends EntityType> subs();

    /**
     * Returns a collection of all Entity instances for this EntityType.
     *
     * @return All the instances of this EntityType.
     * see Entity
     */
    @Override
    Stream<Entity> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default EntityType asEntityType() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isEntityType() {
        return true;
    }
}
