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
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 *
 * <p>
 * Answer iterator for resolution plans (list of atomic queries) coming from {@link ReasonerQueryImpl}.
 * For an input resolution plan it recursively creates an answer iterator by combining each constituent {@link ReasonerAtomicQueryIterator}
 * and merging the resultant answers.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class ReasonerQueryImplCumulativeIterator extends ReasonerQueryIterator{
    private final Answer partialSub;

    private final LinkedList<ReasonerQueryImpl> nextList;
    private final QueryCache<ReasonerAtomicQuery> cache;
    private final Set<ReasonerAtomicQuery> subGoals;

    private final Iterator<Answer> feederIterator;
    private Iterator<Answer> queryIterator;

    ReasonerQueryImplCumulativeIterator(Answer sub, LinkedList<ReasonerQueryImpl> qs,
                                        Set<ReasonerAtomicQuery> subGoals,
                                        QueryCache<ReasonerAtomicQuery> cache){
        this.subGoals = subGoals;
        this.cache = cache;
        this.partialSub = sub;
        this.nextList = Lists.newLinkedList(qs);

        Iterator<Answer> iterator = nextList.removeFirst().extendedIterator(sub, subGoals, cache);

        this.queryIterator = nextList.isEmpty()? iterator : Collections.emptyIterator();
        this.feederIterator = nextList.isEmpty()? Collections.emptyIterator() : iterator;
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;

        if (feederIterator.hasNext() && !nextList.isEmpty()) {
            Answer feederSub  = feederIterator.next();
            queryIterator = new ReasonerQueryImplCumulativeIterator(feederSub.merge(partialSub, true), nextList, subGoals, cache);
            return hasNext();
        }
        return false;
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next();
        sub = sub.merge(partialSub, true);
        return sub;
    }
}
