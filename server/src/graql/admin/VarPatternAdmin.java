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

package grakn.core.graql.admin;

import grakn.core.graql.concept.Label;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Admin class for inspecting a {@link VarPattern}
 *
 */
public interface VarPatternAdmin extends PatternAdmin, VarPattern {
    @Override
    default boolean isVarPattern() {
        return true;
    }

    @Override
    default VarPatternAdmin asVarPattern() {
        return this;
    }

    /**
     * @return the variable name of this variable
     */
    @CheckReturnValue
    Var var();

    /**
     * Get a stream of all properties on this variable
     */
    @CheckReturnValue
    Stream<VarProperty> getProperties();

    /**
     * Get a stream of all properties of a particular type on this variable
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    @CheckReturnValue
    <T extends VarProperty> Stream<T> getProperties(Class<T> type);

    /**
     * Get a unique property of a particular type on this variable, if it exists
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    @CheckReturnValue
    <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type);

    /**
     * Get whether this {@link VarPattern} has a {@link VarProperty} of the given type
     * @param type the type of the {@link VarProperty}
     * @param <T> the type of the {@link VarProperty}
     * @return whether this {@link VarPattern} has a {@link VarProperty} of the given type
     */
    @CheckReturnValue
    <T extends VarProperty> boolean hasProperty(Class<T> type);

    /**
     * @return the name this variable represents, if it represents something with a specific name
     */
    @CheckReturnValue
    Optional<Label> getTypeLabel();

    /**
     * @return all variables that this variable references
     */
    @CheckReturnValue
    Collection<VarPatternAdmin> innerVarPatterns();

    /**
     * Get all inner variables, including implicit variables such as in a has property
     */
    @CheckReturnValue
    Collection<VarPatternAdmin> implicitInnerVarPatterns();

    /**
     * @return all type names that this variable refers to
     */
    @CheckReturnValue
    Set<Label> getTypeLabels();

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    @CheckReturnValue
    String getPrintableName();

}
