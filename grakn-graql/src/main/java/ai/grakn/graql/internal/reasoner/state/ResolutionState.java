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
public abstract class ResolutionState {

    private final Answer sub;
    private final Unifier unifier;

    private final ReasonerQueryImpl query;
    private final ResolutionState parentState;

    ResolutionState(ReasonerQueryImpl query, Answer sub, Unifier u, ResolutionState parent){
        this.query = query;
        this.sub = sub;
        this.unifier = u;
        this.parentState = parent;
    }

    ResolutionState(ResolutionState state){
        this.query = state.query;
        this.sub = state.sub;
        this.unifier = state.unifier;
        this.parentState = state.parentState;
    }

    public abstract ResolutionState copy();

    public abstract ResolutionState generateSubGoal();

    public Answer getSubstitution(){ return sub;}

    public ResolutionState merge(AnswerState state){
        throw new IllegalStateException("dupa");
    }

    public Unifier getUnifier(){ return unifier;}
    public ReasonerQueryImpl getQuery(){ return query;}
    ResolutionState getParentState(){ return parentState;}

    public boolean isAnswerState(){ return false;}
    public boolean isAtomicState(){ return false;}

    public boolean isTopState(){
        return parentState == null && !isAnswerState();
    }
}
