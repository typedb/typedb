/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.ValuePredicate;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Optional;
import java.util.Set;

/**
 * Admin class for inspecting a ValuePredicate
 */
public interface ValuePredicateAdmin extends ValuePredicate {

    @Override
    default ValuePredicateAdmin admin() {
        return this;
    }

    /**
     * @return whether this atom is specific (e.g. "eq" is specific, "regex" is not)
     */
    default boolean isSpecific() {
        return false;
    }

    /**
     * @return the value comparing against, if this is an "equality" atom, otherwise nothing
     */
    default Optional<Object> equalsValue() {
        return Optional.empty();
    }

    /**
     * @return all values referred to in the atom (including within 'ors' and 'ands')
     */
    Set<Object> getInnerValues();

    /**
     * @return the gremlin atom object this ValuePredicate wraps
     */
    P<Object> getPredicate();
}
