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
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.MultiUnifierImpl;
import ai.grakn.graql.internal.reasoner.ResolutionPlan;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.JoinExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Query state corresponding to a conjunctive query ({@link ReasonerQueryImpl}) in the resolution tree.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ConjunctiveState extends QueryState {

    private final ReasonerQueryImpl query;
    private final LinkedList<ReasonerQueryImpl> subQueries;
    private final Iterator<Answer> dbIterator;

    private boolean visited = false;
    private static final Logger LOG = LoggerFactory.getLogger(ConjunctiveState.class);

    public ConjunctiveState(ReasonerQueryImpl q,
                            Answer sub,
                            Unifier u,
                            QueryState parent,
                            Set<ReasonerAtomicQuery> subGoals,
                            QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.query = ReasonerQueries.create(q, sub);

        if (!query.isRuleResolvable()){
            dbIterator = query.getQuery().stream()
                    .map(at -> at.explain(new JoinExplanation(query, at)))
                    .iterator();
            subQueries = new LinkedList<>();
        } else {
            dbIterator = Collections.emptyIterator();
            subQueries = new ResolutionPlan(query).queryPlan();

            LOG.trace("CQ plan:\n" + subQueries.stream()
                    .map(aq -> aq.toString() + (aq.isRuleResolvable()? "*" : ""))
                    .collect(Collectors.joining("\n"))
            );
        }
    }

    @Override
    ReasonerQueryImpl getQuery(){return query;}

    @Override
    MultiUnifier getCacheUnifier() { return new MultiUnifierImpl();}

    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState()) : null;
    }

    @Override
    public ResolutionState generateSubGoal(){
        if (dbIterator.hasNext()){
            return new AnswerState(dbIterator.next(), getUnifier(), getParentState());
        }

        if (!subQueries.isEmpty() && !visited) {
            visited = true;
            return new CumulativeState(subQueries, new QueryAnswer(), getUnifier(), this, getSubGoals(), getCache());
        }
        return null;
    }
}
