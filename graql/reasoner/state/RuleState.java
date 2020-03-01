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

import com.google.common.collect.Iterators;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Resolution state corresponding to a rule application.
 * </p>
 *
 *
 */
public class RuleState extends AnswerPropagatorState<ResolvableQuery> {

    private final InferenceRule rule;

    public RuleState(InferenceRule rule, ConceptMap sub, Unifier unifier, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> visitedSubGoals) {
        super(rule.getBody(), sub, unifier, parent, visitedSubGoals);
        this.rule = rule;
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        //NB; sub gets propagated to the body here
        return Iterators.singletonIterator(getQuery().resolutionState(getSubstitution(), getUnifier(), this, getVisitedSubGoals()));
    }

    @Override
    public String toString(){
        return super.toString() + " to state @" + Integer.toHexString(getParentState().hashCode()) + "\n" +
                rule + "\n" +
                "Unifier: " + getUnifier();
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        ConceptMap answer = consumeAnswer(state);
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState(), rule) : null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
