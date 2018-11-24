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

package grakn.core.graql.query.predicate;

import grakn.core.graql.query.pattern.VarPatternAdmin;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import javax.annotation.CheckReturnValue;
import java.util.Optional;

/**
 * a atom on a value in a query.
 * <p>
 * A atom describes a atom (true/false) function that can be tested against some value in the graph.
 * <p>
 * Predicates can be combined together using the methods {@code and}, {@code or}, {@code any} and {@code all}.
 *
 */
public interface ValuePredicate {

    /**
     * @return whether this predicate is specific (e.g. "eq" is specific, "regex" is not)
     */
    @CheckReturnValue
    default boolean isSpecific() {
        return false;
    }

    /**
     * @param predicate to be compared in terms of compatibility (non-zero overlap of answer sets)
     * @return true if compatible
     */
    @CheckReturnValue
    boolean isCompatibleWith(ValuePredicate predicate);

    /**
     * @param predicate to be compared in terms of subsumption
     * @return true if this predicate subsumes (specialises) the predicate
     */
    @CheckReturnValue
    boolean subsumes(ValuePredicate predicate);

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
