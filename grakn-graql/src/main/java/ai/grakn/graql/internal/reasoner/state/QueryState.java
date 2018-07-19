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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;

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
    private final Iterator<ResolutionState> subGoalIterator;
    private final Supplier<MultiUnifier> cacheUnifierSupplier;
    private MultiUnifier cacheUnifier = null;

    QueryState(Q query, Answer sub, Unifier u, Supplier<MultiUnifier> cus, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.query = query;
        this.subGoalIterator = query.queryStateIterator(this, subGoals, cache);
        this.cacheUnifierSupplier = cus;
    }

    @Override
    public String toString(){
        return getClass() + "\n" + getQuery() + "\n";
    }

    @Override
    public ResolutionState generateSubGoal() {
        return subGoalIterator.hasNext()? subGoalIterator.next() : null;
    }

    /**
     * @return query corresponding to this query state
     */
    Q getQuery(){ return query;}

    /**
     * @return cache unifier if any
     */
    MultiUnifier getCacheUnifier(){
        if (cacheUnifier == null) this.cacheUnifier = cacheUnifierSupplier.get();
        return cacheUnifier;
    }
}
