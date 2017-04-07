/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.concept;

import ai.grakn.exception.ConceptException;

import java.util.Collection;

/**
 * <p>
 *     A Type represents any ontological element in the graph.
 * </p>
 *
 * <p>
 *     Types are used to model the behaviour of {@link Instance} and how they relate to each other.
 *     They also aid in categorising {@link Instance} to different types.
 * </p>
 *
 * @see EntityType
 * @see RoleType
 * @see RelationType
 * @see ResourceType
 * @see RuleType
 *
 * @author fppt
 *
 */
public interface Type extends Concept {
    //------------------------------------- Modifiers ----------------------------------
    // TODO: Describe behaviour when setting a type with direct instances as abstract
    /**
     * Sets the Entity Type to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The concept itself
     *
     * @throws ConceptException if this is a meta-type
     */
    Type setAbstract(Boolean isAbstract) throws ConceptException;

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     *
     * @throws ConceptException if this is a meta-type
     */
    Type plays(RoleType roleType) throws ConceptException;

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     *
     * @throws ConceptException if this is a meta-type
     */
    Type key(ResourceType resourceType) throws ConceptException;

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     *
     * @throws ConceptException if this is a meta-type
     */
     Type resource(ResourceType resourceType) throws ConceptException;

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    Type scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    Type deleteScope(Instance scope);

    //------------------------------------- Accessors ---------------------------------

    /**
     * Returns the unique label of this Type.
     *
     * @return The unique label of this type
     */
    TypeLabel getLabel();

    /**
     *
     * @return A list of Role Types which instances of this Type can indirectly play.
     */
    Collection<RoleType> plays();

    /**
     *
     * @return The resource types which this type is linked with.
     */
    Collection<ResourceType> resources();

    /**
     *
     * @return The resource types which this type is linked with as a key.
     */
    Collection<ResourceType> keys();

    /**
     *
     * @return The direct super of this Type
     */
    Type superType();

    /**
     * Get all indirect sub-types of this type.
     *
     * The indirect sub-types are the type itself and all indirect sub-types of direct sub-types.
     *
     * @return All the indirect sub-types of this Type
     */
    Collection<? extends Type> subTypes();

    /**
     * Get all indirect instances of this type.
     *
     * The indirect instances are the direct instances and all indirect instances of direct sub-types.
     *
     * @return All the indirect instances of this type.
     */
    Collection<? extends Instance> instances();

    /**
     * Return if the type is set to abstract.
     *
     * By default, types are not abstract.
     *
     * @return returns true if the type is set to be abstract.
     */
    Boolean isAbstract();

    /**
     * Return whether the Type was created implicitly.
     *
     * By default, types are not implicit.
     *
     * @return returns true if the type was created implicitly through {@link #resource}
     */
    Boolean isImplicit();

    /**
     * Return the collection of Rules for which this Type serves as a hypothesis.
     * @see Rule
     *
     * @return A collection of Rules for which this Type serves as a hypothesis
     */
    Collection<Rule> getRulesOfHypothesis();

    /**
     * Return the collection of Rules for which this Type serves as a conclusion.
     * @see Rule
     *
     * @return A collection of Rules for which this Type serves as a conclusion
     */
    Collection<Rule> getRulesOfConclusion();

    /**
     * Retrieve a list of the Instances that scope this Type.
     *
     * @return A list of the Instances that scope this Type.
     */
    Collection<Instance> scopes();

    //------------------------------------- Other ----------------------------------
    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    Type deletePlays(RoleType roleType);

    /**
     *
     * @return a deep copy of this concept.
     */
    Type copy();
}
