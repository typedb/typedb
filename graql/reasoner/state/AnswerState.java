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
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

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

    InferenceRule getRule(){ return rule;}

    Unifier getUnifier(){ return unifier;}
}
