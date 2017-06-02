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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.util.Pair;
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

    private final Iterator<Answer> queryIterator;
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    ReasonerQueryImplIterator(ReasonerQueryImpl q,
                              Answer sub,
                              Set<ReasonerAtomicQuery> subGoals,
                              QueryCache<ReasonerAtomicQuery> cache){

        ReasonerQueryImpl query = new ReasonerQueryImpl(q);
        query.addSubstitution(sub);

        //LOG.trace("CQ: " + query);
        //LOG.trace("CQ plan: " + query.getResolutionPlan());

        LinkedList<ReasonerAtomicQuery> queries = query.getResolutionPlan();
        this.queryIterator = new ReasonerAtomicQueryCumulativeIterator(new QueryAnswer(), queries, subGoals, cache);
    }

    @Override
    public boolean hasNext() {
        return queryIterator.hasNext();
    }

    @Override
    public Answer next() {
        return queryIterator.next();
    }
}
