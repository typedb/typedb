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

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * State producing {@link AtomicState}s:
 * - typed {@link AtomicState}s if type inference is required
 * - {@link AtomicState} for non-negated non-ambiguous {@link ReasonerAtomicQuery}
 * - {@link NeqComplementState} for non-ambiguous {@link ReasonerAtomicQuery} with negation
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AtomicStateProducer extends QueryStateBase {

    private final Iterator<ResolutionState> subGoalIterator;

    public AtomicStateProducer(ReasonerAtomicQuery query, ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);

        if(query.getAtom().getSchemaConcept() == null){
            this.subGoalIterator = query.subGoals(sub, u, parent, subGoals, cache).iterator();
        } else {
            this.subGoalIterator = Iterators.singletonIterator(
                    query.getAtoms(NeqPredicate.class).findFirst().isPresent() ?
                            new NeqComplementState(query, sub, u, parent, subGoals, cache) :
                            new AtomicState(query, sub, u, parent, subGoals, cache)
            );
        }
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

