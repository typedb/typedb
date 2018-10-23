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

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.unifier.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.cache.SimpleQueryCache;
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
                            ConceptMap sub,
                            Unifier u,
                            QueryStateBase parent,
                            Set<ReasonerAtomicQuery> visitedSubGoals,
                            SimpleQueryCache<ReasonerAtomicQuery> cache) {
        super(ReasonerQueries.create(q, sub), sub, u, MultiUnifierImpl::new, parent, visitedSubGoals, cache);
    }

    @Override
    MultiUnifier getCacheUnifier() { return new MultiUnifierImpl();}

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        ConceptMap answer = state.getAnswer();
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState()) : null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
