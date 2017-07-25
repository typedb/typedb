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
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.LinkedList;

/**
 *
 */
class CumulativeState extends ResolutionState{

    private final LinkedList<ReasonerQueryImpl> subQueries;
    private final QueryCache<ReasonerAtomicQuery> cache;

    CumulativeState(ReasonerQueryImpl query, LinkedList<ReasonerQueryImpl> qs, Answer sub, Unifier u, ResolutionState parent, QueryCache<ReasonerAtomicQuery> cache) {
        super(query, sub, u, parent);
        this.subQueries = qs;
        this.cache = cache;
    }

    private CumulativeState(CumulativeState state){
        super(state);
        this.subQueries = state.subQueries;
        this.cache = state.cache;
    }

    @Override
    public ResolutionState copy() {
        return new CumulativeState(this);
    }

    @Override
    public ResolutionState merge(AnswerState state) {
        return new CumulativeState(
                getQuery(),
                subQueries,
                getSubstitution().merge(state.getSubstitution(), true),
                getUnifier(),
                getParentState(),
                cache
        );
    }

    @Override
    public ResolutionState generateSubGoal(){
        if (!subQueries.isEmpty()){
            /*


             */
            LinkedList<ReasonerQueryImpl> queries = new LinkedList<>(subQueries);
            ReasonerQueryImpl subQuery = queries.removeFirst();
            ResolutionState cumulativeState = new CumulativeState(getQuery(), queries, getSubstitution(), getUnifier(), this, cache);
            return subQuery.subGoal(getSubstitution(), getUnifier(), cumulativeState, cache);
        } else {
            //this is always an answer to parent conjunctive query
            return new AnswerState(getQuery(), getSubstitution(), getUnifier(), getParentState());
        }
    }
}
