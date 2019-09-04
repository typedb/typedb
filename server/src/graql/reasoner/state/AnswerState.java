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

package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.tree.Node;
import grakn.core.graql.reasoner.tree.NodeSet;
import grakn.core.graql.reasoner.tree.ResolutionTree;
import grakn.core.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Resolution state holding an answer (ConceptMap) to the parent state.
 * </p>
 *
 *
 */
public class AnswerState extends ResolutionState {

    private final InferenceRule rule;
    private final Unifier unifier;

    public AnswerState(ConceptMap sub, Unifier u, AnswerPropagatorState parent) {
        this(sub, u, parent, null);
    }

    AnswerState(ConceptMap sub, Unifier u, AnswerPropagatorState parent, InferenceRule rule) {
        super(sub, parent);
        this.unifier = u;
        this.rule = rule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnswerState that = (AnswerState) o;
        return Objects.equals(getSubstitution(), that.getSubstitution()) &&
                Objects.equals(unifier, that.unifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSubstitution(), unifier);
    }

    @Override
    public String toString(){
        return super.toString() + ": " + getSubstitution() +
                (getParentState() != null? " to @" + Integer.toHexString(getParentState().hashCode()) : "") +
                (" with u: " + getUnifier());
    }

    @Override
    public boolean isAnswerState(){ return true;}

    @Override
    public ResolutionState generateChildState() {
        return getParentState().propagateAnswer(this);
    }


    @Override
    public void updateTreeProfile(ResolutionTree tree){
        AnswerPropagatorState parentState = getParentState();
        //TODO this mapping is ambiguous for MultiNode states
        Node parent = tree.getNode(parentState);
        if (parent != null){
            //Set<Variable> vars = parentState.getQuery().getVarNames();
            //parent.addAnswer(getSubstitution().project(vars));
            parent.addAnswer(getSubstitution());
        }
    }

    InferenceRule getRule(){ return rule;}

    Unifier getUnifier(){ return unifier;}
}
