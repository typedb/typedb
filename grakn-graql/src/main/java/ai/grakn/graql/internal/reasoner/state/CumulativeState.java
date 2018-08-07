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
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.SimpleQueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
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
    private final ReasonerQueryImpl query;

    public CumulativeState(List<ReasonerQueryImpl> qs,
                           ConceptMap sub,
                           Unifier u,
                           QueryStateBase parent,
                           Set<ReasonerAtomicQuery> subGoals,
                           SimpleQueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.subQueries = new LinkedList<>(qs);

        this.query = subQueries.getFirst();
        //NB: we need lazy subGoal initialisation here, otherwise they are marked as visited before visit happens
        this.feederStateIterator = !subQueries.isEmpty()?
                subQueries.removeFirst().subGoals(sub, u, this, subGoals, cache).iterator() :
                Collections.emptyIterator();
    }

    @Override
    public String toString(){
        return getClass() + "\n" +
                getSubstitution() + "\n" +
                query + "\n" +
                subQueries.stream().map(ReasonerQueryImpl::toString).collect(Collectors.joining("\n")) + "\n";
    }

    @Override
    public ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = getSubstitution().merge(state.getSubstitution(), true);
        if (answer.isEmpty()) return null;
        if (subQueries.isEmpty()) return new AnswerState(answer, getUnifier(), getParentState());
        return new CumulativeState(subQueries, answer, getUnifier(), getParentState(), getVisitedSubGoals(), getCache());
    }

    @Override
    public ResolutionState generateSubGoal(){
        return feederStateIterator.hasNext()? feederStateIterator.next() : null;
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}
