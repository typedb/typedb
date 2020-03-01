/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;

/**
 *
 * <p>
 * Base abstract class for resolution states.
 *
 * All resolution states have an ability to generate states - produce further resolvable states in the resolution tree.
 * </p>
 *
 *
 */
public abstract class ResolutionState {

    private final ConceptMap sub;
    private final AnswerPropagatorState parentState;

    ResolutionState(ConceptMap sub, AnswerPropagatorState parent){
        this.sub = sub;
        this.parentState = parent;
    }

    @Override
    public String toString(){ return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());}

    /**
     * @return new sub goal generated from this state
     */
    public abstract ResolutionState generateChildState();

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
    AnswerPropagatorState getParentState(){ return parentState;}
}
