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


package grakn.core.graql.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.plan.ResolutionPlan;
import grakn.core.graql.reasoner.plan.ResolutionQueryPlan;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.ConjunctiveState;
import grakn.core.graql.reasoner.state.ResolutionState;
import java.util.LinkedHashSet;

/**
 *
 */
public class ResolutionNode{
    private final ResolutionState state;
    private final LinkedHashSet<ResolutionNode> children = new LinkedHashSet<>();
    private final LinkedHashSet<ConceptMap> answers = new LinkedHashSet<>();
    private final ResolutionPlan plan;
    private final ResolutionQueryPlan qplan;
    private long totalTime;

    ResolutionNode(ResolutionState state){
        this.state = state;
        this.plan = (state instanceof AnswerPropagatorState)? ((AnswerPropagatorState) state).getQuery().resolutionPlan() : null;
        this.qplan = (state instanceof ConjunctiveState)? ((ConjunctiveState) state).getQuery().resolutionQueryPlan() : null;
    }

    void addChild(ResolutionNode child){
        if(child.state.isAnswerState()) answers.add(child.getState().getSubstitution());
        children.add(child);
    }

    long totalTime(){ return totalTime;}
    void ackCompletion(){ totalTime = System.currentTimeMillis() - state.creationTime();}
    ResolutionState getState(){ return state;}


    @Override
    public String toString(){
        return state.getClass().getSimpleName() + "@" + Integer.toHexString(state.hashCode())
                + " Cost:" + totalTime() +
                " Sub: " + state.getSubstitution();
    }
}
