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
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.JoinExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.ResolutionPlan;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConjunctiveState extends ResolutionState {

    private final LinkedList<ReasonerQueryImpl> subQueries;
    private final Iterator<Answer> dbIterator;

    private final QueryCache<ReasonerAtomicQuery> cache;

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    public ConjunctiveState(ReasonerQueryImpl query,
                            Answer sub,
                            Unifier u,
                            ResolutionState parent,
                            QueryCache<ReasonerAtomicQuery> cache) {
        super(query, sub, u, parent);
        this.cache = cache;

        if (!getQuery().isRuleResolvable()){
            dbIterator = query.getMatchQuery().stream()
                    .map(at -> at.explain(new JoinExplanation(query, at)))
                    .iterator();
            subQueries = new LinkedList<>();
        } else {
            dbIterator = Collections.emptyIterator();
            subQueries = ResolutionPlan.getResolutionPlanFromTraversal(query);

            LOG.trace("CQ plan:\n" + subQueries.stream()
                    .map(aq -> aq.toString() + (aq.isRuleResolvable()? "*" : ""))
                    .collect(Collectors.joining("\n"))
            );
        }

    }

    private ConjunctiveState(ConjunctiveState state){
        super(state);
        this.subQueries = state.subQueries;
        this.dbIterator = Collections.emptyIterator();
        this.cache = state.cache;
    }

    @Override
    public ResolutionState copy() {
        return new ConjunctiveState(this);
    }

    @Override
    public ResolutionState generateSubGoal(){
        if (dbIterator.hasNext())
            return new AnswerState(getQuery(), dbIterator.next(), getUnifier(), this);

        if (!subQueries.isEmpty()){
            LinkedList<ReasonerQueryImpl> queries = new LinkedList<>(subQueries);
            ReasonerQueryImpl subQuery = queries.removeFirst();
            ResolutionState cumulativeState = new CumulativeState(getQuery(), queries, getSubstitution(), getUnifier(), this, cache);
            return subQuery.subGoal(getSubstitution(), getUnifier(), cumulativeState, cache);
        }

        return null;
    }
}
