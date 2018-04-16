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
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Resolution state corresponding to a rule application.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RuleState extends QueryStateBase{

    private final InferenceRule rule;
    private final Iterator<ResolutionState> bodyIterator;

    public RuleState(InferenceRule rule, Answer sub, Unifier unifier, QueryStateBase parent, Set<ReasonerAtomicQuery> visitedSubGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, unifier, parent, visitedSubGoals, cache);
        this.bodyIterator = Iterators.singletonIterator(rule.getBody().subGoal(sub, unifier, this, visitedSubGoals, cache));
        this.rule = rule;
    }


    @Override
    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState(), rule) : null;
    }

    @Override
    public ResolutionState generateSubGoal() {
        return bodyIterator.hasNext() ? bodyIterator.next() : null;
    }

    @Override
    Answer consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
