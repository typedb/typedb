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
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;

/**
 * Query state corresponding to a conjunctive query (ReasonerQueryImpl) in the resolution tree.
 */
public class ConjunctiveState extends AnswerPropagatorState<ReasonerQueryImpl> {

    public ConjunctiveState(ReasonerQueryImpl q,
                            ConceptMap sub,
                            Unifier u,
                            AnswerPropagatorState parent,
                            Set<ReasonerAtomicQuery> visitedSubGoals) {
        super(q.withSubstitution(sub), sub, u, parent, visitedSubGoals);
    }

    @Override
    public String toString(){ return super.toString() + "\n" + getQuery() + "\n"; }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return getQuery().innerStateIterator(this, getVisitedSubGoals());
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = consumeAnswer(state);
        return !answer.isEmpty() ? new AnswerState(answer, getUnifier(), getParentState()) : null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        ConceptMap substitution = state.getSubstitution();
        return new ConceptMap(substitution.map(), substitution.explanation(), getQuery().withSubstitution(substitution).getPattern());
    }
}