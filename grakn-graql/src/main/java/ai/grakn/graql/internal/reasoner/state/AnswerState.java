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
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;

/**
 *
 */
class AnswerState extends ResolutionState {

    AnswerState(ReasonerQueryImpl q, Answer sub, Unifier u, ResolutionState parent) {
        super(q, sub, u, parent);
    }

    private AnswerState(AnswerState state){
        super(state);
    }

    @Override
    public ResolutionState copy() {
        return new AnswerState(this);
    }

    @Override
    public ResolutionState generateSubGoal() {
        ResolutionState parentState = getParentState();

        //generate answer state to parent query
        if (parentState.isAtomicState()) {
            return new AnswerState(
                    parentState.getQuery(),
                    getSubstitution()
                            .unify(getUnifier())
                            .filterVars(getQuery().getVarNames()),
                    parentState.getUnifier(),
                    parentState.getParentState()
            );
        } else {
            return parentState.merge(this);
        }
    }

    @Override
    public boolean isAnswerState(){ return true;}
}
