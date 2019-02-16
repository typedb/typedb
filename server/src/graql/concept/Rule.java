/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.concept;

import graql.lang.pattern.Pattern;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * A {@link SchemaConcept} used to model and categorise {@link Rule}s.
 */
public interface Rule extends SchemaConcept {
    //------------------------------------- Accessors ----------------------------------
    /**
     * Retrieves the when part of the {@link Rule}
     * When this query is satisfied the "then" part of the rule is executed.
     *
     * @return A string representing the left hand side Graql query.
     */
    @CheckReturnValue
    @Nullable
    Pattern when();

    /**
     * Retrieves the then part of the {@link Rule}.
     * This query is executed when the "when" part of the rule is satisfied
     *
     * @return A string representing the right hand side Graql query.
     */
    @CheckReturnValue
    @Nullable
    Pattern then();

    /**
     * Retrieve a set of {@link Type}s that constitute a part of the hypothesis of this {@link Rule}.
     *
     * @return A collection of Concept {@link Type}s that constitute a part of the hypothesis of the {@link Rule}
     */
    @CheckReturnValue
    Stream<Type> whenTypes();

    /**
     * Retrieve a set of {@link Type}s that constitue a part of the conclusion of the {@link Rule}.
     *
     * @return A collection of {@link Type}s that constitute a part of the conclusion of the {@link Rule}
     */
    @CheckReturnValue
    Stream<Type> thenTypes();

    //------------------------------------- Modifiers ----------------------------------
    /**
     * Changes the {@link Label} of this {@link Concept} to a new one.
     * @param label The new {@link Label}.
     * @return The {@link Concept} itself
     */
    Rule label(Label label);

    /**
     *
     * @return The super of this {@link Rule}
     */
    @Nullable
    @Override
    Rule sup();

    /**
     *
     * @param superRule The super of this {@link Rule}
     * @return The {@link Rule} itself
     */
    Rule sup(Rule superRule);

    /**
     * @return All the super-types of this this {@link Rule}
     */
    @Override
    Stream<Rule> sups();

    /**
     *
     * @return All the sub of this {@link Rule}
     */
    @Override
    Stream<Rule> subs();

    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default Rule asRule(){
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isRule(){
        return true;
    }
}
