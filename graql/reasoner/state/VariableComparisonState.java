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
import grakn.core.graql.reasoner.atom.predicate.VariablePredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * Query state corresponding to an atomic query (ReasonerAtomicQuery) with variable comparison predicates -
 * either NeqIdPredicates or VariableValuePredicates.
 * We define the entry query Q as a conjunction:
 *
 * Q := Q', P
 *
 * where
 * Q' is the entry query stripped of all comparison predicates
 * P are the comparison predicates
 *
 * Consequently we compute the answer set of Q' ans(Q') first and find the answer set of Q ans(Q) by applying all variable
 * predicates to ans(Q').
 *
 */
public class VariableComparisonState extends AnswerPropagatorState<ReasonerQueryImpl> {

    private final ConceptMap variablePredicateSub;
    private final Set<VariablePredicate> variablePredicates;

    public VariableComparisonState(ReasonerQueryImpl q,
                                   ConceptMap sub,
                                   Unifier u,
                                   AnswerPropagatorState parent,
                                   Set<ReasonerAtomicQuery> subGoals) {
        super(q.withSubstitution(sub), sub, u, parent, subGoals);

        this.variablePredicates = getQuery().getAtoms(VariablePredicate.class).collect(Collectors.toSet());
        this.variablePredicateSub = AnswerUtil.joinAnswers(getQuery().getSubstitution(), sub)
                .project(this.variablePredicates.stream().flatMap(p -> p.getVarNames().stream()).collect(Collectors.toSet()));
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return Iterators.singletonIterator(
                getQuery().constantValuePredicateQuery().resolutionState(getSubstitution(), getUnifier(), this, getVisitedSubGoals())
        );
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap fullAnswer = AnswerUtil.joinAnswers(state.getSubstitution(), variablePredicateSub);

        boolean predicatesSatisfied = variablePredicates.stream()
                .allMatch(p -> p.isSatisfied(fullAnswer));
        return predicatesSatisfied?
                new AnswerState(state.getSubstitution(), getUnifier(), getParentState()) :
                null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
