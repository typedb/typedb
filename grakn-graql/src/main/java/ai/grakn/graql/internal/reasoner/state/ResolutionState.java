/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.ConceptMap;

/**
 *
 * <p>
 * Base abstract class for resolution states.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class ResolutionState {

    private final ConceptMap sub;
    private final QueryStateBase parentState;

    ResolutionState(ConceptMap sub, QueryStateBase parent){
        this.sub = sub;
        this.parentState = parent;
    }

    /**
     * @return new sub goal generated from this state
     */
    public abstract ResolutionState generateSubGoal();

    /**
     * @return substitution this state has
     */
    public ConceptMap getSubstitution(){ return sub;}

    /**
     * @return true if this resolution state is an answer state
     */
    public boolean isAnswerState(){ return false;}

    /**
     * @return true if this state is a top resolution state
     */
    public boolean isTopState(){
        return parentState == null;
    }

    /**
     * @return parent state of this state
     */
    QueryStateBase getParentState(){ return parentState;}
}
