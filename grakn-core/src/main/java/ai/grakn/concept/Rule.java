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
 * </p>
 *
 * @author fppt
 */
public interface Rule extends SchemaConcept {
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the Left Hand Side of the rule.
     * When this query is satisfied the "then" part of the rule is executed.
     *
     * @return A string representing the left hand side Graql query.
     */
    @CheckReturnValue
    Pattern getWhen();

    /**
     * Retrieves the Right Hand Side of the rule.
     * This query is executed when the "when" part of the rule is satisfied
     *
     * @return A string representing the right hand side Graql query.
     */
    @CheckReturnValue
    Pattern getThen();

    /**
     * Retrieve a set of Types that constitute a part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the Rule
     */
    @CheckReturnValue
    Stream<Type> getHypothesisTypes();

    /**
     * Retrieve a set of Types that constitue a part of the conclusion of the Rule.
     *
     * @return A collection of Concept Types that constitute a part of the conclusion of the Rule
     */
    @CheckReturnValue
    Stream<Type> getConclusionTypes();

    //------------------------------------- Modifiers ----------------------------------
    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    Rule setLabel(Label label);

    /**
     *
     * @return The super type of this {@link Rule}
     */
    @Override
    @Nonnull
    Rule sup();

    /**
     *
     * @param type The super type of this {@link Rule}
     * @return The {@link Rule} itself
     */
    Rule sup(Rule type);

    /**
     * Adds another subtype to this type
     *
     * @param type The sub type of this {@link Rule}
     * @return The Rule itself
     */
    Rule sub(Rule type);

    /**
     *
     * @return All the sub types of this {@link Rule}
     */
    @Override
    Stream<Rule> subs();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Rule asRuleType(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRuleType(){
        return true;
    }
}
