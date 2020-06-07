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

import graql.lang.pattern.Pattern;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * A SchemaConcept used to model and categorise Rules.
 */
public interface Rule extends SchemaConcept {
    //------------------------------------- Accessors ----------------------------------

    /**
     * Retrieves the when part of the Rule
     * When this query is satisfied the "then" part of the rule is executed.
     *
     * @return A string representing the left hand side Graql query.
     */
    @CheckReturnValue
    @Nullable
    Pattern when();

    /**
     * Retrieves the then part of the Rule.
     * This query is executed when the "when" part of the rule is satisfied
     *
     * @return A string representing the right hand side Graql query.
     */
    @CheckReturnValue
    @Nullable
    Pattern then();

    /**
     * Retrieve a set of Types that constitute a part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a part of the hypothesis of the Rule
     */
    @CheckReturnValue
    Stream<Type> whenTypes();

    /**
     * Retrieve a set of Types that constitute a positive part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a positive part of the hypothesis of the Rule
     */
    @CheckReturnValue
    Stream<Type> whenPositiveTypes();

    /**
     * Retrieve a set of Types that constitute a negative part of the hypothesis of this Rule.
     *
     * @return A collection of Concept Types that constitute a negative part of the hypothesis of the Rule
     */
    @CheckReturnValue
    Stream<Type> whenNegativeTypes();

    /**
     * Retrieve a set of Types that constitue a part of the conclusion of the Rule.
     *
     * @return A collection of Types that constitute a part of the conclusion of the Rule
     */
    @CheckReturnValue
    Stream<Type> thenTypes();

    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    Rule label(Label label);

    /**
     * @return The super of this Rule
     */
    @Nullable
    @Override
    Rule sup();

    /**
     * @param superRule The super of this Rule
     * @return The Rule itself
     */
    Rule sup(Rule superRule);

    /**
     * @return All the super-types of this this Rule
     */
    @Override
    Stream<Rule> sups();

    /**
     * @return All the sub of this Rule
     */
    @Override
    Stream<Rule> subs();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Rule asRule() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRule() {
        return true;
    }
}
