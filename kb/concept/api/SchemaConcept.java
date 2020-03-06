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
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Facilitates construction of ontological elements.
 * Allows you to create schema or ontological elements.
 * These differ from normal graph constructs in two ways:
 * 1. They have a unique Label which identifies them
 * 2. You can link them together into a hierarchical structure
 */
public interface SchemaConcept extends Concept {
    //------------------------------------- Modifiers ----------------------------------

    /**
     * Changes the Label of this Concept to a new one.
     *
     * @param label The new Label.
     * @return The Concept itself
     */
    SchemaConcept label(Label label);

    //------------------------------------- Accessors ---------------------------------

    /**
     * Returns the unique id of this Type.
     *
     * @return The unique id of this type
     */
    @CheckReturnValue
    LabelId labelId();

    /**
     * Returns the unique label of this Type.
     *
     * @return The unique label of this type
     */
    @CheckReturnValue
    Label label();

    /**
     * @return The direct super of this concept
     */
    @CheckReturnValue
    @Nullable
    SchemaConcept sup();

    /**
     * @return All super-concepts of this SchemaConcept  including itself and excluding the meta
     * Schema.MetaSchema#THING.
     * If you want to include Schema.MetaSchema#THING, use Transaction.sups().
     */
    Stream<? extends SchemaConcept> sups();

    /**
     * Get all indirect subs of this concept.
     * The indirect subs are the concept itself and all indirect subs of direct subs.
     *
     * @return All the indirect sub-types of this SchemaConcept
     */
    @CheckReturnValue
    Stream<? extends SchemaConcept> subs();

    /**
     * Return whether the SchemaConcept was created implicitly.
     * By default, SchemaConcept are not implicit.
     *
     * @return returns true if the type was created implicitly through the Attribute syntax
     */
    @CheckReturnValue
    Boolean isImplicit();

    /**
     * Return the collection of Rule for which this SchemaConcept serves as a hypothesis.
     *
     * @return A collection of Rule for which this SchemaConcept serves as a hypothesis
     * see Rule
     */
    @CheckReturnValue
    Stream<Rule> whenRules();

    /**
     * Return the collection of Rule for which this SchemaConcept serves as a conclusion.
     *
     * @return A collection of Rule for which this SchemaConcept serves as a conclusion
     * see Rule
     */
    @CheckReturnValue
    Stream<Rule> thenRules();


    //------------------------------------- Other ---------------------------------
    @Deprecated
    @CheckReturnValue
    @Override
    default SchemaConcept asSchemaConcept() {
        return this;
    }

    @Deprecated
    @CheckReturnValue
    @Override
    default boolean isSchemaConcept() {
        return true;
    }
}
