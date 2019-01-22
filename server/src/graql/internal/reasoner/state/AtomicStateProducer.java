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

import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * State producing {@link AtomicState}s when when atom type is unknown and type inference is required
 * </p>
 *
 *
 */
public class AtomicStateProducer extends QueryStateBase {

    private final Iterator<ResolutionState> subGoalIterator;

    public AtomicStateProducer(ReasonerAtomicQuery query, ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache) {
        super(sub, u, parent, subGoals, cache);
        this.subGoalIterator = query.subGoals(sub, u, parent, subGoals, cache).iterator();
    }

    @Override
    public ResolutionState generateSubGoal() {
        return subGoalIterator.hasNext() ? subGoalIterator.next() : null;
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        return new AnswerState(state.getSubstitution(), state.getUnifier(), getParentState());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}

