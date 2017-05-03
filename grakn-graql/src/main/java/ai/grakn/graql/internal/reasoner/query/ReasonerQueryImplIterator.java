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
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;


/**
 *
 * <p>
 * Tuple-at-a-time iterator for ReasonerQueryImpl.
 * For a starting query Q it removes the top (highest priority) atom A, constructs a corresponding atomic query
 * AQ and uses it to feed the the remaining query Q' = Q\AQ with partial substitutions. The behaviour proceeds
 * in recursive fashion.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class ReasonerQueryImplIterator extends ReasonerQueryIterator {

    private final Answer partialSubstitution;
    private final ReasonerQueryImpl queryPrime;

    private final QueryCache<ReasonerAtomicQuery> cache;
    private final Set<ReasonerAtomicQuery> subGoals;

    private Iterator<Answer> queryIterator;
    private final Iterator<Answer> atomicQueryIterator;

    ReasonerQueryImplIterator(ReasonerQueryImpl query,
                              Answer sub,
                              Set<ReasonerAtomicQuery> subGoals,
                              QueryCache<ReasonerAtomicQuery> cache){
        this.partialSubstitution = sub;
        this.subGoals = subGoals;
        this.cache = cache;

        //get prioritised atom and construct atomic query from it
        this.queryPrime = new ReasonerQueryImpl(query);
        queryPrime.addSubstitution(sub);
        Atom topAtom = queryPrime.getTopAtom();
        ReasonerAtomicQuery q = new ReasonerAtomicQuery(topAtom);

        boolean isAtomic = queryPrime.isAtomic();
        if (!isAtomic) queryPrime.removeAtom(topAtom);

        atomicQueryIterator = isAtomic? Collections.emptyIterator() : q.iterator(subGoals, cache);
        queryIterator = isAtomic? q.iterator(subGoals, cache) : Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;
        else {
            if (atomicQueryIterator.hasNext()) {
                Answer sub = atomicQueryIterator.next();
                queryIterator = getQueryPrime().iterator(sub, subGoals, cache);
                return hasNext();
            }
            else return false;
        }
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next();
        sub = sub.merge(partialSubstitution, true);
        return sub;
    }

    private ReasonerQueryImpl getQueryPrime(){
        return queryPrime.isAtomic()? new ReasonerAtomicQuery(queryPrime.getTopAtom()) : queryPrime;
    }
}
