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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * Tuple-at-a-time iterator for {@link ReasonerQueryImpl}.
 * For a starting query Q it removes the top (highest priority) atom A, constructs a corresponding atomic query
 * AQ and uses it to feed the the remaining query Q' = Q\AQ with partial substitutions. The behaviour proceeds
 * in recursive fashion.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class ReasonerQueryImplIterator extends ReasonerQueryIterator {

    private Answer partialSub = new QueryAnswer();
    private final ReasonerQueryImpl queryPrime;

    private final QueryCache<ReasonerAtomicQuery> cache;
    private final Set<ReasonerAtomicQuery> subGoals;

    private final Iterator<Answer> atomicQueryIterator;
    private Iterator<Answer> queryIterator = Collections.emptyIterator();

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    ReasonerQueryImplIterator(ReasonerQueryImpl q,
                              Answer sub,
                              Set<ReasonerAtomicQuery> subGoals,
                              QueryCache<ReasonerAtomicQuery> cache){
        this.subGoals = subGoals;
        this.cache = cache;

        //get prioritised atom and construct atomic query from it
        ReasonerQueryImpl query = new ReasonerQueryImpl(q);
        query.addSubstitution(sub);
        Atom topAtom = query.getTopAtom();

        LOG.trace("CQ: " + query);
        LOG.trace("CQ delta: " + sub);
        LOG.trace("CQ plan: " + query.getResolutionPlan());
        
        this.atomicQueryIterator = new ReasonerAtomicQuery(topAtom).iterator(new QueryAnswer(), subGoals, cache);
        this.queryPrime = ReasonerQueries.prime(query, topAtom);
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;

        if (atomicQueryIterator.hasNext()) {
            partialSub = atomicQueryIterator.next();
            queryIterator = queryPrime.iterator(partialSub, subGoals, cache);
            return hasNext();
        }
        else return false;
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next();
        sub = sub.merge(partialSub, true);
        return sub;
    }
}
