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
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Query state corresponding to a an intermediate state obtained from decomposing a conjunctive query ({@link ReasonerQueryImpl}) in the resolution tree.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class CumulativeState extends QueryStateBase{

    private final LinkedList<ReasonerQueryImpl> subQueries;
    private final Iterator<ResolutionState> feederStateIterator;

    public CumulativeState(LinkedList<ReasonerQueryImpl> qs,
                           Answer sub,
                           Unifier u,
                           QueryStateBase parent,
                           Set<ReasonerAtomicQuery> subGoals,
                           QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.subQueries = new LinkedList<>(qs);

        if (subQueries.isEmpty()){
            this.feederStateIterator = Collections.emptyIterator();
        } else {

            ReasonerQueryImpl query = subQueries.removeFirst();
            //ResolutionState resolutionState = query.subGoal(sub, u, this, subGoals, cache);
            //List<ResolutionState> sgs = query.subGoals(sub, u, this, subGoals, cache).collect(Collectors.toList());
            //this.feederStateIterator = !subQueries.isEmpty() ?
                    //Iterators.singletonIterator(subQueries.removeFirst().subGoal(sub, u, this, subGoals, cache)) :
                    //Iterators.singletonIterator(subQueries.removeFirst().subGoals(sub, u, this, subGoals, cache).iterator().next()) :
            this.feederStateIterator = query.subGoals(sub, u, this, subGoals, cache).iterator();
            //this.feederStateIterator =  Iterators.singletonIterator(resolutionState);
                    //Collections.emptyIterator();
        }
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        Answer answer = getSubstitution().merge(state.getSubstitution(), true);
        if (subQueries.isEmpty()){
            return new AnswerState(answer, getUnifier(), getParentState());
        }
        return new CumulativeState(subQueries, answer, getUnifier(), getParentState(), getVisitedSubGoals(), getCache());
    }

    @Override
    public ResolutionState generateSubGoal(){
        return feederStateIterator.hasNext()? feederStateIterator.next() : null;
    }

    @Override
    Answer consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
