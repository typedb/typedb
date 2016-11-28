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

package ai.grakn.graql.admin;

import ai.grakn.graql.Var;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Admin class for inspecting a Var
 */
public interface VarAdmin extends PatternAdmin, Var {
    @Override
    default boolean isVar() {
        return true;
    }

    @Override
    default VarAdmin asVar() {
        return this;
    }

    /**
     * @return the variable name of this variable
     */
    String getVarName();

    /**
     * @param name the new variable name of this variable
     */
    void setVarName(String name);

    /**
     * @return whether the user specified a name for this variable
     */
    boolean isUserDefinedName();

    /**
     * Get a stream of all properties on this variable
     */
    Stream<VarProperty> getProperties();

    /**
     * Get a stream of all properties of a particular type on this variable
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    <T extends VarProperty> Stream<T> getProperties(Class<T> type);

    /**
     * Get a unique property of a particular type on this variable, if it exists
     * @param type the class of {@link VarProperty} to return
     * @param <T> the type of {@link VarProperty} to return
     */
    <T extends UniqueVarProperty> Optional<T> getProperty(Class<T> type);

    /**
     * Get whether this {@link Var} has a {@link VarProperty} of the given type
     * @param type the type of the {@link VarProperty}
     * @param <T> the type of the {@link VarProperty}
     * @return whether this {@link Var} has a {@link VarProperty} of the given type
     */
    <T extends VarProperty> boolean hasProperty(Class<T> type);

    /**
     * @return the ID this variable represents, if it represents something with a specific ID
     */
    Optional<String> getId();

    /**
     * @return the name this variable represents, if it represents something with a specific name
     */
    Optional<String> getTypeName();

    /**
     * @return all variables that this variable references
     */

    Set<VarAdmin> getInnerVars();

    /**
     * Get all inner variables, including implicit variables such as in a has-resource property
     */
    Set<VarAdmin> getImplicitInnerVars();

    /**
     * @return all type names that this variable refers to
     */
    Set<String> getTypeNames();

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    String getPrintableName();

}
