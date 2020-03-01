/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Base abstract class for resolution states that exhibit the following behaviours:
 *
 * - answer propagation
 *   The state serves as a proxy state for propagating answer states up the tree to the root state.
 *   When an answer state propagates via this state, it is consumed - a specific action is performed on the answer state.
 *
 * - possibility of production of multiple states
 *   The state has internal query states coming from an underlying query.
 *   The state defines an iterator for inner states that's used for state generation.
 *
 * </p>
 *
 */
public abstract class AnswerPropagatorState<Q extends ResolvableQuery> extends ResolutionState {

    private final Q query;
    private final Unifier unifier;
    private final Set<ReasonerAtomicQuery> visitedSubGoals;
    private final Iterator<ResolutionState> subGoalIterator;

    AnswerPropagatorState(Q query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(sub, parent);
        this.query = query;
        this.unifier = u;
        this.visitedSubGoals = subGoals;
        this.subGoalIterator = generateChildStateIterator();
    }

    @Override
    public ResolutionState generateChildState() {
        return subGoalIterator.hasNext()? subGoalIterator.next() : null;
    }

    /**
     * @return query corresponding to this query state
     */
    Q getQuery(){ return query;}

    /**@return set of already visited subGoals (atomic queries)
     */
    Set<ReasonerAtomicQuery> getVisitedSubGoals(){ return visitedSubGoals;}

    /**
     * @return unifier of this state with parent state
     */
    public Unifier getUnifier(){ return unifier;}

    /**
     *
     * @return
     */
    abstract Iterator<ResolutionState> generateChildStateIterator();

    /**
     * propagates the answer state up the tree and acknowledges (caches) its substitution
     * @param state to propagate
     * @return new resolution state obtained by propagating the answer up the resolution tree
     */
    abstract ResolutionState propagateAnswer(AnswerState state);

    /**
     * @param state answer state providing the answer
     * @return digested (acknowledged and cached) answer
     */
    abstract ConceptMap consumeAnswer(AnswerState state);
}
