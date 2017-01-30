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

import ai.grakn.graql.admin.ValuePredicateAdmin;

/**
 * a atom on a value in a query.
 * <p>
 * A atom describes a atom (true/false) function that can be tested against some value in the graph.
 * <p>
 * Predicates can be combined together using the methods {@code and}, {@code or}, {@code any} and {@code all}.
 *
 * @author Felix Chapman
 */
public interface ValuePredicate {

    /**
     * @return an Admin class allowing inspection of this atom
     */
    ValuePredicateAdmin admin();

}
