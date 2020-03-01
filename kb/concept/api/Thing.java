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
import java.util.stream.Stream;

/**
 * A data instance in the graph belonging to a specific Type
 * Instances represent data in the graph.
 * Every instance belongs to a Type which serves as a way of categorising them.
 * Instances can relate to one another via Relation
 */
public interface Thing extends Concept {
    //------------------------------------- Accessors ----------------------------------

    /**
     * Return the Type of the Concept.
     *
     * @return A Type which is the type of this concept. This concept is an instance of that type.
     */
    @CheckReturnValue
    Type type();

    /**
     * Retrieves a Relations which the Thing takes part in, which may optionally be narrowed to a particular set
     * according to the Role you are interested in.
     *
     * @param roles An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     * see Role
     * see Relation
     */
    @CheckReturnValue
    Stream<Relation> relations(Role... roles);

    /**
     * Determine the Roles that this Thing is currently playing.
     *
     * @return A set of all the Roles which this Thing is currently playing.
     * see Role
     */
    @CheckReturnValue
    Stream<Role> roles();

    /**
     * Creates a Relation from this Thing to the provided Attribute.
     * This has the same effect as #relhas(Attribute), but returns the instance itself to allow
     * method chaining.
     *
     * @param attribute The Attribute to which a Relation is created
     * @return The instance itself
     */
    Thing has(Attribute attribute);

    /**
     * Creates a Relation from this instance to the provided Attribute.
     * This has the same effect as #has(Attribute), but returns the new Relation.
     *
     * @param attribute The Attribute to which a Relation is created
     * @return The Relation connecting the Thing and the Attribute
     */
    Relation relhas(Attribute attribute);

    /**
     * Retrieves a collection of Attribute attached to this Thing
     *
     * @param attributeTypes AttributeTypes of the Attributes attached to this entity
     * @return A collection of AttributeTypes attached to this Thing.
     * see Attribute
     */
    @CheckReturnValue
    Stream<Attribute<?>> attributes(AttributeType... attributeTypes);

    /**
     * Retrieves a collection of Attribute attached to this Thing as a key
     *
     * @param attributeTypes AttributeTypes of the Attributes attached to this entity
     * @return A collection of AttributeTypes attached to this Thing.
     * see Attribute
     */
    @CheckReturnValue
    Stream<Attribute<?>> keys(AttributeType... attributeTypes);

    /**
     * Removes the provided Attribute from this Thing
     *
     * @param attribute the Attribute to be removed
     * @return The Thing itself
     */
    Thing unhas(Attribute attribute);

    /**
     * Used to indicate if this Thing has been created as the result of a Rule inference.
     *
     * @return true if this Thing exists due to a rule
     * see Rule
     */
    boolean isInferred();


    /**
     * Add an inferred attribute ownership to this Thing
     */
    Relation attributeInferred(Attribute attribute);

    /**
     * Return concepts that are DIRECT dependants of this concept - concepts required to be persisted if we persist this concept.
     */
    default Stream<Thing> getDependentConcepts(){ return Stream.of(this);}

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Thing asThing() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isThing() {
        return true;
    }
}
