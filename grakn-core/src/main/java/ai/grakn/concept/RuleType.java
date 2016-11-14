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

import ai.grakn.graql.Pattern;

import java.util.Collection;

/**
 * An ontological element used to define different types of rule.
 * Currently supported rules include Constraint Rules and Inference Rules.
 */
public interface RuleType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * @param lhs A string representing the left hand side Graql query.
     * @param rhs A string representing the right hand side Graql query.
     * @return a new Rule
     */
    Rule addRule(Pattern lhs, Pattern rhs);

    //---- Inherited Methods
    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Rule Type itself
     */
    RuleType setAbstract(Boolean isAbstract);

    /**
     *
     * @return The super type of this Rule Type
     */
    RuleType superType();

    /**
     *
     * @param type The super type of this Rule Type
     * @return The Rule Type itself
     */
    RuleType superType(RuleType type);

    /**
     *
     * @return All the sub types of this rule type
     */
    Collection<RuleType> subTypes();

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Rule Type itself
     */
    RuleType playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Rule Type itself
     */
    RuleType deletePlaysRole(RoleType roleType);

    /**
     *
     * @return All the rule instances of this Rule Type.
     */
    Collection<Rule> instances();
}
