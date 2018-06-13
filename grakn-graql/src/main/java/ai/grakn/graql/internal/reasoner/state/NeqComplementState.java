/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
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

    private final Answer predicateSub;
    private final ResolutionState complementState;
    private boolean visited = false;

    private final Set<NeqPredicate> predicates;

    NeqComplementState(ReasonerAtomicQuery q,
                       Answer sub,
                       Unifier u,
                       QueryStateBase parent,
                       Set<ReasonerAtomicQuery> subGoals,
                       QueryCache<ReasonerAtomicQuery> cache) {
        super(q, sub, u, parent, subGoals, cache);

        ReasonerAtomicQuery complementQuery = ReasonerQueries.atomic(q.positive(), sub);
        this.predicates = q.getAtoms(NeqPredicate.class).collect(Collectors.toSet());
        this.predicateSub = sub.project(this.predicates.stream().flatMap(p -> p.getVarNames().stream()).collect(Collectors.toSet()));

        complementState = complementQuery.subGoal(sub, u, this, subGoals, cache);
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        Answer fullAnswer = state.getSubstitution().merge(predicateSub);

        boolean isNeqSatisfied = predicates.stream()
                .allMatch(p -> p.isSatisfied(fullAnswer));
        return isNeqSatisfied?
                new AnswerState(state.getSubstitution(), getUnifier(), getParentState()) :
                null;
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
