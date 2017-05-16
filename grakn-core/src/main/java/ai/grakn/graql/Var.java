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
 *
 */

package ai.grakn.graql;

import java.util.function.Function;

/**
 * A variable name in a Graql query
 *
 * @author Felix Chapman
 */
public interface Var extends VarPatternBuilder {

    /**
     * Get the string name of the variable (without prefixed "$")
     */
    String getValue();

    /**
     * Rename a variable (does not modify the original {@link Var})
     * @param mapper a function to apply to the underlying variable name
     * @return the new variable name
     */
    Var map(Function<String, String> mapper);

    VarPattern pattern();

    /**
     * Get a shorter representation of the variable (with prefixed "$")
     */
    String shortName();

    String toString();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();
}
