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
import grakn.core.graql.reasoner.query.CompositeQuery;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Query state corresponding to a conjunctive query with negated patterns present(CompositeQuery).
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
public class CompositeState extends AnswerPropagatorState<CompositeQuery> {

    private final Set<ResolvableQuery> complements;

    public CompositeState(CompositeQuery query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(query.withSubstitution(sub), sub, u, parent, subGoals);
        this.complements = getQuery().getComplementQueries();
    }

    @Override
    public String toString(){
        return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + "\n" + getQuery().toString();
    }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return getQuery().innerStateIterator(this, getVisitedSubGoals());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) { return state.getSubstitution(); }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = consumeAnswer(state);

        boolean isNegationSatisfied = complements.stream()
                .map(q -> q.withSubstitution(answer))
                .noneMatch(q -> q.resolve(getVisitedSubGoals(), true).findFirst().isPresent());

        return isNegationSatisfied?
                new AnswerState(answer, getUnifier(), getParentState()) :
                null;
    }
}
