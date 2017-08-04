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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.stream.Stream;

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
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    RuleType setLabel(Label label);

    /**
     * Adds a new Rule if it does not exist otherwise returns the existing rule.
     * @see Pattern
     *
     * @param when A string representing the when part of the {@link Rule}
     * @param then A string representing the then part of the {@link Rule}
     * @return a new Rule
     */
    Rule putRule(Pattern when, Pattern then);

    /**
     * Classifies the type to a specific scope. This allows you to optionally categorise types.
     *
     * @param scope The category of this Type
     * @return The Type itself.
     */
    @Override
    RuleType scope(Thing scope);

    /**
     * Delete the scope specified.
     *
     * @param scope The Instances that is currently scoping this Type.
     * @return The Type itself
     */
    @Override
    RuleType deleteScope(Thing scope);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked in a strictly one-to-one mapping.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RuleType key(ResourceType resourceType);

    /**
     * Creates a RelationType which allows this type and a resource type to be linked.
     *
     * @param resourceType The resource type which instances of this type should be allowed to play.
     * @return The Type itself.
     */
    @Override
    RuleType resource(ResourceType resourceType);

    //---- Inherited Methods
    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Rule Type itself
     */
    @Override
    RuleType setAbstract(Boolean isAbstract);

    /**
     *
     * @return The super type of this Rule Type
     */
    @Override
    @Nonnull
    RuleType sup();

    /**
     *
     * @param type The super type of this Rule Type
     * @return The Rule Type itself
     */
    RuleType sup(RuleType type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this rule type
     * @return The RuleType itself
     */
    RuleType sub(RuleType type);

    /**
     *
     * @return All the sub types of this rule type
     */
    @Override
    Stream<RuleType> subs();

    /**
     *
     * @param role The Role Type which the instances of this Type are allowed to play.
     * @return The Rule Type itself
     */
    @Override
    RuleType plays(Role role);

    /**
     *
     * @param role The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Rule Type itself
     */
    @Override
    RuleType deletePlays(Role role);

    /**
     *
     * @return All the rule instances of this Rule Type.
     */
    @Override
    Stream<Rule> instances();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default RuleType asRuleType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRuleType(){
        return true;
    }
}
