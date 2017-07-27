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

/**
 *
 */
public abstract class ResolutionState {

    private final Answer sub;
    private final Unifier unifier;
    private final QueryState parentState;

    ResolutionState(Answer sub, Unifier u, QueryState parent){
        this.sub = sub;
        this.unifier = u;
        this.parentState = parent;
    }

    /**
     *
     * @return
     */
    public abstract ResolutionState generateSubGoal();

    /**
     *
     * @return
     */
    public Answer getSubstitution(){ return sub;}


    /**
     *
     * @return
     */
    public boolean isAnswerState(){ return false;}

    /**
     *
     * @return
     */
    public boolean isTopState(){
        return parentState == null;
    }

    /**
     *
     * @return
     */
    Unifier getUnifier(){ return unifier;}

    /**
     *
     * @return
     */
    QueryState getParentState(){ return parentState;}
}
