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
import java.util.Set;

/**
 *
 */
class AnswerState extends ResolutionState {

    AnswerState(
                Answer sub,
                Unifier u,
                ResolutionState parent,
                Set<ReasonerAtomicQuery> subGoals,
                QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
    }

    private AnswerState(AnswerState state){
        super(state);
    }

    @Override
    public ResolutionState copy() {
        return new AnswerState(this);
    }

    @Override
    public boolean isAnswerState(){ return true;}

    @Override
    public ResolutionState generateSubGoal() {
        ResolutionState parentState = getParentState();

        //TODO save to cache

        if (parentState instanceof AtomicState){
            getCache().recordAnswer(((AtomicState) parentState).getQuery(), getSubstitution());
        }

        //generate answer state to parent query
        return parentState.propagateAnswer(this);
    }


}
