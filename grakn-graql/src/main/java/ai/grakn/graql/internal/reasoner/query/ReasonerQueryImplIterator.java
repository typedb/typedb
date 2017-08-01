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
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.JoinExplanation;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * Tuple-at-a-time iterator for {@link ReasonerQueryImpl}, wraps around a {@link ReasonerQueryImplCumulativeIterator}.
 * For a starting conjunctive query Q it constructs a resolution plan by decomposing it to atomic queries {@link ReasonerAtomicQuery}
 * ordering them by their resolution priority. The ordered list is then passed to {@link ReasonerQueryImplCumulativeIterator}
 * which takes care of substitution propagation leading to a final answer.
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

        queryIterator = getQueryIterator(query, subGoals, cache);
    }

    private Iterator<Answer> getQueryIterator(ReasonerQueryImpl query,
                                              Set<ReasonerAtomicQuery> subGoals,
                                              QueryCache<ReasonerAtomicQuery> cache){
        if (!query.isRuleResolvable()){
            return query.getMatchQuery().stream()
                    .map(at -> at.explain(new JoinExplanation(query, at)))
                    .iterator();
        }

        LinkedList<ReasonerQueryImpl> queries = ResolutionPlan.getResolutionPlanFromTraversal(query);

        LOG.trace("CQ plan:\n" + queries.stream()
                .map(aq -> aq.toString() + (aq.isRuleResolvable()? "*" : ""))
                .collect(Collectors.joining("\n"))
        );

        return new ReasonerQueryImplCumulativeIterator(new QueryAnswer(), queries, subGoals, cache);
    }


    @Override
    public boolean hasNext() {
        return queryIterator.hasNext();
    }

    @Override
    public Answer next() { return queryIterator.next();}
}
