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
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.QueryStateIterator;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * <p>
 * Specialised class for resolution states corresponding to different forms of queries.
 * </p>
 *
 * @param <Q> the type of query that this state is corresponding to
 *
 * @author Kasper Piskorski
 *
 */
public abstract class QueryState<Q extends ReasonerQueryImpl> extends QueryStateBase{

    private final Q query;
    private final Iterator<Answer> dbIterator;
    private final Iterator<QueryStateBase> subGoalIterator;
    private final MultiUnifier cacheUnifier;

    QueryState(Q query, Answer sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.query = query;

        QueryStateIterator queryStateIterator = query.queryStateIterator(this, subGoals, cache);
        this.dbIterator = queryStateIterator.dbIterator();
        this.cacheUnifier = queryStateIterator.cacheUnifier();
        this.subGoalIterator = queryStateIterator.subGoalIterator();
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (dbIterator.hasNext()){
            return new AnswerState(dbIterator.next(), getUnifier(), this);
        }
        return subGoalIterator.hasNext()? subGoalIterator.next() : null;
    }

    /**
     * @return true if this state corresponds to an atomic state
     */
    boolean isAtomicState(){ return false; }

    /**
     * @return query corresponding to this query state
     */
    Q getQuery(){ return query;}

    /**
     * @return cache unifier if any
     */
    MultiUnifier getCacheUnifier(){ return cacheUnifier;}
}
