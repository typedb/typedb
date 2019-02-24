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

package grakn.core.graql.internal.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.unifier.Unifier;

import java.util.Set;

/**
 *
 * <p>
 * Base abstract class for resolution states corresponding to different forms of queries.
 * </p>
 *
 *
 */
public abstract class QueryStateBase extends ResolutionState {

    private final Unifier unifier;
    private final Set<ReasonerAtomicQuery> visitedSubGoals;
    private final MultilevelSemanticCache cache;

    QueryStateBase(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache) {
        super(sub, parent);
        this.unifier = u;
        this.visitedSubGoals = subGoals;
        this.cache = cache;
    }

    /**
     * @return set of already visited subGoals (atomic queries)
     */
    Set<ReasonerAtomicQuery> getVisitedSubGoals(){ return visitedSubGoals;}

    /**
     * @return query cache
     */
    MultilevelSemanticCache getCache(){ return cache;}

    /**
     * @return unifier of this state with parent state
     */
    public Unifier getUnifier(){ return unifier;}

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
