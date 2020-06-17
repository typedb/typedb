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
import grakn.core.graql.reasoner.explanation.DisjunctiveExplanation;
import grakn.core.graql.reasoner.query.DisjunctiveQuery;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class DisjunctiveState extends AnswerPropagatorState<DisjunctiveQuery> {

    public DisjunctiveState(DisjunctiveQuery query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(query, sub, u, parent, subGoals);
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return getQuery().innerStateIterator(this, getVisitedSubGoals());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        ConceptMap sub = state.getSubstitution();
        return new ConceptMap(sub.map(), new DisjunctiveExplanation(sub), getQuery().withSubstitution(sub).getPattern());
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = consumeAnswer(state);

        HashMap<Variable, Concept> outerScopeVarsSub = getQuery().filterBindingVars(answer.map());

        return !answer.isEmpty() ?
                new AnswerState(
                        new ConceptMap(outerScopeVarsSub, answer.explanation(), getQuery().getPattern(outerScopeVarsSub)),
                        getUnifier(),
                        getParentState()
                )
                : null;
    }
}
