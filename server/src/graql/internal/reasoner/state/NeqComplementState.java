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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.cache.IndexedSemanticCache;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Query state corresponding to an atomic query ({@link ReasonerAtomicQuery}) with neq predicates ({@link NeqPredicate}).
 * Defining the answer to the entry query Q as a set B:
 *
 * B = Ans{R(x1, x2, ..., xn), xj = ..., xi != xj}
 *
 * We find the answer to the query by finding the complement:
 *
 * B = A\C,
 *
 * where
 *
 * A = Ans{R(x1, x2, ..., xn), xj = ...}
 * C = Ans{R(x1, x2, ..., xn), xj = ..., xi = xj}
 *
 * </p>
 *
 *
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
public class NeqComplementState extends QueryStateBase {

    private final AtomicState complementState;
    private boolean visited = false;

    private final ConceptMap neqPredicateSub;
    private final Set<NeqPredicate> neqPredicates;

    NeqComplementState(ReasonerAtomicQuery q,
                       ConceptMap sub,
                       Unifier u,
                       QueryStateBase parent,
                       Set<ReasonerAtomicQuery> subGoals,
                       IndexedSemanticCache cache) {
        super(sub, u, parent, subGoals, cache);
        ReasonerAtomicQuery query = ReasonerQueries.atomic(q, sub);
        this.neqPredicates = q.getAtoms(NeqPredicate.class).collect(Collectors.toSet());
        this.neqPredicateSub = query.getSubstitution().merge(sub)
                .project(this.neqPredicates.stream().flatMap(p -> p.getVarNames().stream()).collect(Collectors.toSet()));

        this.complementState = new AtomicState(query.positive(), sub, u, this, subGoals, cache);
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap fullAnswer = state.getSubstitution().merge(neqPredicateSub);

        boolean isNeqSatisfied = neqPredicates.stream()
                .allMatch(p -> p.isSatisfied(fullAnswer));
        return isNeqSatisfied?
                new AnswerState(state.getSubstitution(), getUnifier(), getParentState()) :
                null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return complementState.consumeAnswer(state);
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
