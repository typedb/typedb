/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */


package grakn.core.graql.reasoner.tree;

import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.stream.Stream;

/**
 *
 */
public class NodeSingle extends Node{
    private final ResolutionState state;
    //private final ResolutionPlan plan;
    //private final ResolutionQueryPlan qplan;

    public NodeSingle(ResolutionState state){
        this.state = state;
        //this.plan = (state instanceof AnswerPropagatorState)? ((AnswerPropagatorState) state).getQuery().resolutionPlan() : null;
        //this.qplan = (state instanceof ConjunctiveState)? ((ConjunctiveState) state).getQuery().resolutionQueryPlan() : null;
    }

    @Override
    public Stream<ResolutionState> getStates(){ return Stream.of(state);}

    @Override
    public void ackCompletion() {
        updateTime(System.currentTimeMillis() - state.creationTime());
    }

    @Override
    public String toString(){
        return state.getClass().getSimpleName() +
                "@" + Integer.toHexString(state.hashCode()) +
                " Cost: " + totalTime() +
                " answers: " + answers().size();
    }

}
