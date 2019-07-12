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

package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.unifier.Unifier;

import java.util.Iterator;
import java.util.Set;

/**
 * Specialised class for resolution states corresponding to different forms of queries.
 *
 * @param <Q> the type of query that this state is corresponding to
 */
public abstract class QueryState<Q extends ResolvableQuery> extends AnswerPropagatorState {

    private final Q query;
    private final Iterator<ResolutionState> subGoalIterator;

    QueryState(Q query, ConceptMap sub, Unifier u, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> subGoals) {
        super(sub, u, parent, subGoals);
        this.query = query;
        this.subGoalIterator = generateSubGoalIterator();
    }

    @Override
    public String toString(){ return super.toString() + "\n" + getQuery() + "\n"; }

    @Override
    public ResolutionState generateSubGoal() {
        return subGoalIterator.hasNext()? subGoalIterator.next() : null;
    }

    protected Iterator<ResolutionState> generateSubGoalIterator() {
        return getQuery().queryStateIterator(this, getVisitedSubGoals());
    }

    /**
     * @return query corresponding to this query state
     */
    Q getQuery(){ return query;}
}
