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

import ai.grakn.graql.ValuePredicate;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import javax.annotation.CheckReturnValue;
import java.util.Optional;

/**
 * Admin class for inspecting a ValuePredicate
 *
 * @author Felix Chapman
 */
public interface ValuePredicateAdmin extends ValuePredicate {

    @Override
    default ValuePredicateAdmin admin() {
        return this;
    }

    /**
     * @return whether this predicate is specific (e.g. "eq" is specific, "regex" is not)
     */
    @CheckReturnValue
    default boolean isSpecific() {
        return false;
    }

    /**
     * @param predicate to be compared in terms of compatibility
     * @return true if compatible
     */
    @CheckReturnValue
    boolean isCompatibleWith(ValuePredicateAdmin predicate);

    /**
     * @return the value comparing against, if this is an "equality" predicate, otherwise nothing
     */
    @CheckReturnValue
    default Optional<Object> equalsValue() {
        return Optional.empty();
    }

    /**
     * @return the gremlin predicate object this ValuePredicate wraps
     */
    @CheckReturnValue
    Optional<P<Object>> getPredicate();

    /**
     * Get the inner variable that this predicate refers to, if one is present
     * @return the inner variable that this predicate refers to, if one is present
     */
    @CheckReturnValue
    Optional<VarPatternAdmin> getInnerVar();

    /**
     * Apply the predicate to the gremlin traversal, so the traversal will filter things that don't meet the predicate
     * @param traversal the traversal to apply the predicate to
     */
    <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal);
}
