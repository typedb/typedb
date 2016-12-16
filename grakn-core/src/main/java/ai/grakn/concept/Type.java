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

import java.util.Collection;

/**
 * Type is an ontological element which represents the categories concepts can fall within.
 *
 * @see EntityType
 * @see RoleType
 * @see RelationType
 * @see ResourceType
 * @see RuleType
 */
public interface Type extends Concept {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Sets the Entity Type to be abstract - which prevents it from having any instances.
     *
     * @param isAbstract  Specifies if the concept is to be abstract (true) or not (false).
     * @return The concept itself
     */
    Type setAbstract(Boolean isAbstract);

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    Type playsRole(RoleType roleType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    RelationType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The resulting relation type which allows instances of this type to have relations with the provided resourceType.
     */
    RelationType hasResource(ResourceType resourceType);

    //------------------------------------- Accessors ---------------------------------

    /**
     * Returns the name of this Type.
     *
     * @return The name of this type
     */
    String getName();

    /**
     *
     * @return A list of Role Types which instances of this Type can play.
     */
    Collection<RoleType> playsRoles();

    /**
     *
     * @return The super of this Type
     */
    Type superType();

    /**
     *
     * @return All the sub classes of this Type
     */
    Collection<? extends Type> subTypes();

    /**
     *
     * @return All the instances of this type.
     */
    Collection<? extends Instance> instances();

    /**
     *
     * @return returns true if the type is set to be abstract.
     */
    Boolean isAbstract();

    /**
     * Return whether the Type was created implicitly.
     *
     * @return returns true if the type was created implicitly through {@link #hasResource}
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

    //------------------------------------- Other ----------------------------------
    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    Type deletePlaysRole(RoleType roleType);
}
