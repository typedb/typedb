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

import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.CompositeQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ResolvableQuery;
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
public class CompositeState extends QueryStateBase {

    private final ResolutionState baseConjunctionState;
    private boolean visited = false;

    private final CompositeQuery query;
    private final Set<ResolvableQuery> complements;

    public CompositeState(CompositeQuery query, ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache) {
        super(sub, u, parent, subGoals, cache);
        this.query = query;
        this.baseConjunctionState = query.getConjunctiveQuery().subGoal(getSubstitution(), getUnifier(), this, getVisitedSubGoals(), getCache());
        this.complements = query.getComplementQueries();
    }

    @Override
    public String toString(){
        return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + "\n" +
                query.toString() +
                (!complements.isEmpty()? "\nNOT{\n" + complements + "\n}" : "");
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) { return state.getSubstitution(); }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = state.getAnswer();

        boolean isNegationSatisfied = complements.stream()
                .map(q -> ReasonerQueries.resolvable(q, answer))
                .noneMatch(q -> q.resolve(getVisitedSubGoals(), getCache(), q.requiresReiteration()).findFirst().isPresent());

        return isNegationSatisfied?
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
