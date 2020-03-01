/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;

/**
 *
 * <p>
 * Iterator for query answers maintaining the iterative behaviour of the QSQ scheme.
 * </p>
 *
 *
 */
public class ResolutionIterator extends ReasonerQueryIterator {

    private int iter = 0;
    private long oldAns = 0;
    private final ResolvableQuery query;
    private final Set<ConceptMap> answers = new HashSet<>();
    private final Set<ReasonerAtomicQuery> subGoals;
    private final QueryCache queryCache;
    private final Stack<ResolutionState> states = new Stack<>();

    private ConceptMap nextAnswer = null;

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionIterator.class);

    public ResolutionIterator(ResolvableQuery q, Set<ReasonerAtomicQuery> subGoals, QueryCache queryCache){
        this.query = q;
        this.subGoals = subGoals;
        this.queryCache = queryCache;
        states.push(query.resolutionState(new ConceptMap(), new UnifierImpl(), null, subGoals));
    }

    private ConceptMap findNextAnswer(){
        while(!states.isEmpty()) {
            ResolutionState state = states.pop();

            LOG.trace("state: {}", state);

            if (state.isAnswerState() && state.isTopState()) {
                return state.getSubstitution();
            }

            ResolutionState newState = state.generateChildState();
            if (newState != null) {
                if (!state.isAnswerState()) states.push(state);
                states.push(newState);
            } else {
                LOG.trace("new state: NULL");
            }
        }
        return null;
    }

    @Override
    public ConceptMap next(){
        if (nextAnswer == null) throw new NoSuchElementException();
        answers.add(nextAnswer);
        ConceptMap toReturn = nextAnswer;
        nextAnswer = null;
        return toReturn;
    }

    private Boolean reiterate = null;

    private boolean reiterate(){
        if (reiterate == null) {
            reiterate = query.requiresReiteration();
        }
        return reiterate;
    }

    /**
     * check whether answers available, if answers not fully computed compute more answers
     * @return true if answers available
     */
    @Override
    public boolean hasNext() {
        if (nextAnswer != null) return true;
        nextAnswer = findNextAnswer();
        if (nextAnswer != null) return true;

        //iter finished
        if (reiterate()) {
            long dAns = answers.size() - oldAns;
            if (dAns != 0 || iter == 0) {
                LOG.debug("iter: {} answers: {} dAns = {}", iter, answers.size(), dAns);
                iter++;
                states.push(query.resolutionState(new ConceptMap(), new UnifierImpl(), null, new HashSet<>()));
                oldAns = answers.size();
                return hasNext();
            }
        }

        MultilevelSemanticCache queryCache = CacheCasting.queryCacheCast(this.queryCache);

        subGoals.forEach(queryCache::ackCompleteness);
        queryCache.propagateAnswers();

        return false;
    }
}
