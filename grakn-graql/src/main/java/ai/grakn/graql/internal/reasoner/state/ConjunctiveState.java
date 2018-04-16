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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;

import java.util.Set;

/**
 *
 * <p>
 * Query state corresponding to a conjunctive query ({@link ReasonerQueryImpl}) in the resolution tree.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ConjunctiveState extends QueryState<ReasonerQueryImpl> {

    public ConjunctiveState(ReasonerQueryImpl q,
                            Answer sub,
                            Unifier u,
                            QueryStateBase parent,
                            Set<ReasonerAtomicQuery> visitedSubGoals,
                            QueryCache<ReasonerAtomicQuery> cache) {
        super(ReasonerQueries.create(q, sub), sub, u, parent, visitedSubGoals, cache);
    }

    @Override
    MultiUnifier getCacheUnifier() { return new MultiUnifierImpl();}

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState()) : null;
    }

    @Override
    Answer consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
