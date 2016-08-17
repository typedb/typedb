/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql;

import io.mindmaps.graql.admin.ValuePredicateAdmin;

/**
 * a predicate on a value in a query.
 * <p>
 * A predicate describes a predicate (true/false) function that can be tested against some value in the graph.
 * <p>
 * Predicates can be combined together using the methods {@code and}, {@code or}, {@code any} and {@code all}.
 */
public interface ValuePredicate {

    /**
     * @param other the other predicate
     * @return a predicate that returns true when both predicates are true
     */
    ValuePredicate and(ValuePredicate other);

    /**
     * @param other the other predicate
     * @return a predicate that returns true when either predicate is true
     */
    ValuePredicate or(ValuePredicate other);

    /**
     * @return an Admin class allowing inspection of this predicate
     */
    ValuePredicateAdmin admin();

}
