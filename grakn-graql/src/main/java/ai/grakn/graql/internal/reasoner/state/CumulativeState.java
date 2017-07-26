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
import java.util.Set;

/**
 *
 */
class CumulativeState extends ResolutionState{

    private final LinkedList<ReasonerQueryImpl> subQueries;
    private final ResolutionState feederGoal;

    private boolean visited = false;

    CumulativeState(LinkedList<ReasonerQueryImpl> qs,
                    Answer sub,
                    Unifier u,
                    ResolutionState parent,
                    Set<ReasonerAtomicQuery> subGoals,
                    QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.subQueries = new LinkedList<>(qs);
        this.feederGoal = !subQueries.isEmpty()? subQueries.removeFirst().subGoal(sub, u, this, subGoals, cache) : null;
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        Answer sub = getSubstitution().merge(state.getSubstitution(), true);
        if (subQueries.isEmpty()){
            return new AnswerState(
                    sub,
                    getUnifier(),
                    getParentState(),
                    getSubGoals(),
                    getCache()
            );
        }
        return new CumulativeState(
                subQueries,
                sub,
                getUnifier(),
                getParentState(),
                getSubGoals(),
                getCache()
        );
    }

    @Override
    public ResolutionState generateSubGoal(){
        if (!visited){
            visited = true;
            return feederGoal;
        }
        return null;
    }
}
