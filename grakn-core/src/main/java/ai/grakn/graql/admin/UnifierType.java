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

/**
 *
 * <p>
 * A class defining different unifier types.
 * </p>
 *
 *@author Kasper Piskorski
 *
 */
public enum UnifierType {
    /**
     * Exact unifier, requires type and id predicate bindings to match.
     */
    EXACT,
    /**
     * Rule unifier, found between queries and rule heads, allows rule heads to be more specific than matched queries.
     */
    RULE,
    /**
     * Similar to rule one with addition to allowing id predicates to differ.
     */
    STRUCTURAL
}
