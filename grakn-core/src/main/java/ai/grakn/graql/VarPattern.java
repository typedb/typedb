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

package ai.grakn.graql;

import ai.grakn.graql.admin.VarPatternAdmin;

/**
 * A wildcard variable to refers to a concept in a query.
 * <p>
 * A {@link VarPattern} may be given a variable name, or left as an "anonymous" variable. {@code Graql} provides
 * static methods for constructing {@link VarPattern} objects.
 * <p>
 * The methods on {@link VarPattern} are used to set its properties. A {@link VarPattern} behaves differently depending
 * on the type of query its used in. In a {@link MatchQuery}, a {@link VarPattern} describes the properties any matching
 * concept must have. In an {@link InsertQuery}, it describes the properties that should be set on the inserted concept.
 * In a {@link DeleteQuery}, it describes the properties that should be deleted.
 *
 * @author Felix Chapman
 */
@SuppressWarnings("UnusedReturnValue")
public interface VarPattern extends Pattern, VarPatternBuilder {

    /**
     * @return an Admin class to allow inspection of this {@link VarPattern}
     */
    VarPatternAdmin admin();
}
