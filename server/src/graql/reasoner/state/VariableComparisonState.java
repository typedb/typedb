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

package grakn.core.graql.reasoner.state;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.predicate.VariablePredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.server.kb.concept.ConceptUtils;

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
@SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
public class VariableComparisonState extends QueryStateBase {

    private final ResolutionState complementState;
    private boolean visited = false;

    private final ConceptMap variablePredicateSub;
    private final Set<VariablePredicate> variablePredicates;

    public VariableComparisonState(ResolvableQuery q,
                                   ConceptMap sub,
                                   Unifier u,
                                   QueryStateBase parent,
                                   Set<ReasonerAtomicQuery> subGoals) {
        super(sub, u, parent, subGoals);
        ResolvableQuery query = ReasonerQueries.resolvable(q, sub);
        this.variablePredicates = q.getAtoms(VariablePredicate.class).collect(Collectors.toSet());
        this.variablePredicateSub = ConceptUtils.mergeAnswers(query.getSubstitution(), sub)
                .project(this.variablePredicates.stream().flatMap(p -> p.getVarNames().stream()).collect(Collectors.toSet()));

        this.complementState = query.neqPositive().subGoal(sub, u, this, subGoals);
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap fullAnswer = ConceptUtils.mergeAnswers(state.getSubstitution(), variablePredicateSub);

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

    @Override
    public ResolutionState generateSubGoal() {
        if (!visited){
            visited = true;
            return complementState;
        }
        return null;
    }
}
