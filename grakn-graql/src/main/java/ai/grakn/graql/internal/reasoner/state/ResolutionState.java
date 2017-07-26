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
import java.util.Set;

/**
 *
 */
public abstract class ResolutionState {

    private final Answer sub;
    private final Unifier unifier;

    private final ResolutionState parentState;

    private final Set<ReasonerAtomicQuery> subGoals;
    private final QueryCache<ReasonerAtomicQuery> cache;

    ResolutionState(Answer sub,
                    Unifier u,
                    ResolutionState parent,
                    Set<ReasonerAtomicQuery> subGoals,
                    QueryCache<ReasonerAtomicQuery> cache){
        this.sub = sub;
        this.unifier = u;
        this.parentState = parent;
        this.subGoals = subGoals;
        this.cache = cache;
    }

    ResolutionState(ResolutionState state){
        this.sub = state.sub;
        this.unifier = state.unifier;
        this.parentState = state.parentState;
        this.subGoals = state.subGoals;
        this.cache = state.cache;
    }

    public abstract ResolutionState copy();

    public abstract ResolutionState generateSubGoal();

    public Answer getSubstitution(){ return sub;}

    public ResolutionState propagateAnswer(AnswerState state){
        throw new IllegalStateException("dupa");
    }

    public Unifier getUnifier(){ return unifier;}

    ResolutionState getParentState(){ return parentState;}
    Set<ReasonerAtomicQuery> getSubGoals(){ return subGoals;}
    QueryCache<ReasonerAtomicQuery> getCache(){ return cache;}

    public boolean isAnswerState(){ return false;}

    public boolean isTopState(){
        return parentState == null;
    }
}
