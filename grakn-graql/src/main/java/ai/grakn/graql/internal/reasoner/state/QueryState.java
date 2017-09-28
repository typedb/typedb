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

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Set;

/**
 *
 * <p>
 * Base abstract class for resolution states corresponding to different forms of queries.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class QueryState extends ResolutionState {

    private final Unifier unifier;
    private final Set<ReasonerAtomicQuery> subGoals;
    private final QueryCache<ReasonerAtomicQuery> cache;

    QueryState(Answer sub, Unifier u, QueryState parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, parent);
        this.unifier = u;
        this.subGoals = subGoals;
        this.cache = cache;
    }

    /**
     * @return true if this state corresponds to an atomic state
     */
    boolean isAtomicState(){ return false; }

    /**
     * @return query corresponding to this query state
     */
    abstract ReasonerQueryImpl getQuery();

    /**
     * @return set of already visited subGoals (atomic queries)
     */
    Set<ReasonerAtomicQuery> getSubGoals(){ return subGoals;}

    /**
     * @return query cache
     */
    QueryCache<ReasonerAtomicQuery> getCache(){ return cache;}

    /**
     * @return cache unifier if any
     */
    abstract MultiUnifier getCacheUnifier();

    /**
     * @return unifier of this state with parent state
     */
    Unifier getUnifier(){ return unifier;}

    /**
     * propagates the answer state up the tree and acknowledges (caches) its substitution
     * @param state to propagate
     * @return new resolution state obtained by propagating the answer up the resolution tree
     */
    abstract ResolutionState propagateAnswer(AnswerState state);
}
