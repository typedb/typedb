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
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;

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
                            MultilevelSemanticCache cache) {
        super(ReasonerQueries.create(q, sub), sub, u, parent, visitedSubGoals, cache);
    }

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