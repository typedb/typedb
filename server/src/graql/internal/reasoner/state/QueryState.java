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

import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.internal.reasoner.query.ReasonerQueryImpl;

import java.util.Iterator;
import java.util.Set;

/**
 * Specialised class for resolution states corresponding to different forms of queries.
 *
 * @param <Q> the type of query that this state is corresponding to
 */
public abstract class QueryState<Q extends ReasonerQueryImpl> extends QueryStateBase{

    private final Q query;
    private final Iterator<ResolutionState> subGoalIterator;

    QueryState(Q query, ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache) {
        super(sub, u, parent, subGoals, cache);
        this.query = query;
        this.subGoalIterator = query.queryStateIterator(this, subGoals, cache);
    }

    @Override
    public String toString(){ return super.toString() + "\n" + getQuery() + "\n"; }

    @Override
    public ResolutionState generateSubGoal() {
        return subGoalIterator.hasNext()? subGoalIterator.next() : null;
    }

    /**
     * @return query corresponding to this query state
     */
    Q getQuery(){ return query;}
}
