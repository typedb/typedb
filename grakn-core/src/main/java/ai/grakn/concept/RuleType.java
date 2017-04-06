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

import ai.grakn.graph.admin.GraknAdmin;
import ai.grakn.graql.Pattern;

import java.util.Collection;

/**
 * <p>
 *     An ontological element used to model and categorise different types of {@link Rule}.
 * </p>
 *
 * <p>
 *     An ontological element used to define different types of {@link Rule}.
 *     Currently supported rules include {@link GraknAdmin#getMetaRuleInference()}
 *     and {@link GraknAdmin#getMetaRuleConstraint()}
 * </p>
 *
 * @author fppt
 */
public interface RuleType extends Type {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * Adds a new Rule if it does not exist otherwise returns the existing rule.
     * @see Pattern
     *
     * @param lhs A string representing the left hand side Graql query.
     * @param rhs A string representing the right hand side Graql query.
     * @return a new Rule
     */
    Rule putRule(Pattern lhs, Pattern rhs);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    RuleType scope(Instance scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    RuleType deleteScope(Instance scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RuleType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    RuleType resource(ResourceType resourceType);

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
     * Adds another subtype to this type
     *
     * @param type The sub type of this rule type
     * @return The RuleType itself
     */
    RuleType subType(RuleType type);

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
    RuleType plays(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Rule Type itself
     */
    RuleType deletePlays(RoleType roleType);

    /**
     *
     * @return All the rule instances of this Rule Type.
     */
    Collection<Rule> instances();

    /**
     *
     * @return a deep copy of this concept.
     */
    RuleType copy();
}
