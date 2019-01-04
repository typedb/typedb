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

package grakn.core.graql.internal.reasoner.state;

import com.google.common.collect.Sets;

import grakn.core.graql.admin.Atomic;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.NegatedAtomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Query state corresponding to a conjunctive query with negated var patterns.
 *
 * Q = A ∧ {...} ∧ ¬B ∧ ¬C ∧ {...}
 *
 * Now each answer x to query Q has to belong to the set:
 *
 * {x : x ∈ A ∧ x !∈ B ∧ x !∈ C ∧ {...}}
 *
 * or equivalently:
 *
 * {x : x ∈ A x ∈ B^C ∧ x ∈ C^C ∧ {...}}
 *
 * where upper C letter marks set complement.
 *
 * As a result the answer set ans(Q) is equal to:
 *
 * ans(Q) = ans(A) \ [ ( ans(A) ∩ ans(B) ) ∪ ( ans(A) ∩ ans(C) ) ]
 *
 * or equivalently
 *
 * ans(Q) = ans(A) ∩ ans(B^C) ∩ ans(C^C)
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class NegatedConjunctiveState extends ConjunctiveState {

    private final ResolutionState baseConjunctionState;
    private boolean visited = false;

    private final Set<ReasonerQueryImpl> complements;

    public NegatedConjunctiveState(ConjunctiveState conjState) {
        super(conjState.getQuery(), conjState.getSubstitution(), conjState.getUnifier(), conjState.getParentState(), conjState.getVisitedSubGoals(), conjState.getCache());

        ReasonerQueryImpl baseConjQuery = ReasonerQueries.create(
                getQuery().getAtoms().stream().filter(Atomic::isPositive).collect(Collectors.toSet()),
                getQuery().tx());

        this.baseConjunctionState = baseConjQuery
                .subGoal(getSubstitution(), getUnifier(), this, getVisitedSubGoals(), getCache());

        this.complements = getQuery().getAtoms(NegatedAtomic.class)
                .filter(at -> !at.isPositive())
                .map(NegatedAtomic::negate)
                .map(at -> complement(at, baseConjQuery))
                .collect(Collectors.toSet());
    }

    private static ReasonerQueryImpl complement(Atomic at, ReasonerQueryImpl baseConjQuery){
        return at.isAtom()?
                ReasonerQueries.atomic((Atom) at) :
                ReasonerQueries.create(Sets.union(baseConjQuery.getAtoms(), Sets.newHashSet(at)), baseConjQuery.tx());
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = state.getAnswer();

        boolean isNegationSatistfied = complements.stream()
                .map(q -> ReasonerQueries.create(q, answer))
                .noneMatch(q -> q.resolve(getCache()).findFirst().isPresent());

        return isNegationSatistfied?
                new AnswerState(answer, getUnifier(), getParentState()) :
                null;
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (!visited){
            visited = true;
            return baseConjunctionState;
        }
        return null;
    }
}
