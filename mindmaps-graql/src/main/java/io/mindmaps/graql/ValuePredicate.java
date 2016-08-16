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

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Optional;
import java.util.Set;

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
    Admin admin();

    /**
     * Admin class for inspecting a ValuePredicate
     */
    interface Admin extends ValuePredicate {
        /**
         * @return whether this predicate is specific (e.g. "eq" is specific, "regex" is not)
         */
        boolean isSpecific();

        /**
         * @return the value comparing against, if this is an "equality" predicate, otherwise nothing
         */
        Optional<Object> equalsValue();

        /**
         * @return all values referred to in the predicate (including within 'ors' and 'ands')
         */
        Set<Object> getInnerValues();

        /**
         * @return the gremlin predicate object this ValuePredicate wraps
         */
        P<Object> getPredicate();
    }
}
