/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.model;

import java.util.Collection;

/**
 * An ontological element which represents categories concepts can fall within.
 */
public interface Type extends Concept {
    //------------------------------------- Modifiers ----------------------------------

    /**
     *
     * @param id The new unique id of the concept.
     * @return The Type itself.
     */
    Type setId(String id);

    /**
     *
     * @param subject The new unique subject of the concept.
     * @return The Type itself.
     */
    Type setSubject(String subject);

    /**
     *
     * @param value The String value to store in the type
     * @return The Type itself
     */
    Type setValue(String value);

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The concept itself
     */
    Type setAbstract(Boolean isAbstract);

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    Type playsRole(RoleType roleType);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The string value stored in the concept
     */
    String getValue();

    /**
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
     * @return Gets all the instances of this type.
     */
    Collection<? extends Concept> instances();

    /**
     *
     * @return returns true if the type is set to be abstract.
     */
    Boolean isAbstract();

    /**
     *
     * @return A collection of Rules for which this Type serves as a hypothesis
     */
    Collection<Rule> getRulesOfHypothesis();

    /**
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
