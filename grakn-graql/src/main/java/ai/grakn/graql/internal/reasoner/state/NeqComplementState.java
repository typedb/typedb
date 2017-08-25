/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
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
 * @author Kasper Piskorski
 *
 */
public class NeqComplementState extends AtomicState {

    private final ReasonerAtomicQuery complementQuery;
    private final AtomicState state;
    private boolean visited = false;

    private final Set<NeqPredicate> predicates;

    public NeqComplementState(ReasonerAtomicQuery q, Answer sub, Unifier u, QueryState parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(q, sub, u, parent, subGoals, cache);
        this.predicates = q.getAtoms(NeqPredicate.class).collect(Collectors.toSet());
        this.complementQuery = q.positive();

        state = new AtomicState(complementQuery, sub, u, this, subGoals, cache);
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        Answer answer = state.getAtomicAnswer(complementQuery, null, getUnifier(), getCache());
        if (answer.isEmpty()) return null;
        ResolutionState answerState =  new AnswerState(answer, getUnifier(), getParentState());

        boolean isNeqSatisfied = !predicates.stream()
                .filter(p -> !p.isSatisfied(answerState.getSubstitution()))
                .findFirst().isPresent();
        return isNeqSatisfied? answerState : null;
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (!visited){
            visited = true;
            return state;
        }
        return null;
    }
}
