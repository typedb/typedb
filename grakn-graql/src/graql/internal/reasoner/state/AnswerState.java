/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

/**
 *
 * <p>
 * Resolution state holding an answer ({@link ConceptMap}) to the parent state.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AnswerState extends ResolutionState {

    private final InferenceRule rule;
    private final Unifier unifier;

    public AnswerState(ConceptMap sub, Unifier u, QueryStateBase parent) {
        this(sub, u, parent, null);
    }

    AnswerState(ConceptMap sub, Unifier u, QueryStateBase parent, InferenceRule rule) {
        super(sub, parent);
        this.unifier = u;
        this.rule = rule;
    }

    @Override
    public boolean isAnswerState(){ return true;}

    @Override
    public ResolutionState generateSubGoal() {
        return getParentState().propagateAnswer(this);
    }

    InferenceRule getRule(){ return rule;}

    Unifier getUnifier(){ return unifier;}

    ConceptMap getAnswer(){ return getParentState().consumeAnswer(this);}
}
