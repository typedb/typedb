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
import grakn.core.graql.reasoner.state.AtomicState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.graql.reasoner.tree.Node;
import grakn.core.graql.reasoner.tree.ResolutionTree;
import grakn.core.graql.reasoner.unifier.UnifierImpl;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Set<ReasonerAtomicQuery> subGoalsCompleteOnFinalise = new HashSet();
    private final QueryCache queryCache;
    private final Stack<ResolutionState> states = new Stack<>();
    private final ResolutionTree logTree;

    private ConceptMap nextAnswer = null;

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionIterator.class);

    public ResolutionIterator(ResolvableQuery q, Set<ReasonerAtomicQuery> subGoals, QueryCache queryCache){
        this.query = q;
        this.queryCache = queryCache;
        ResolutionState rootState = query.resolutionState(new ConceptMap(), new UnifierImpl(), null, subGoals);
        states.push(rootState);
        this.logTree = new ResolutionTree(rootState);
    }

    private ConceptMap findNextAnswer(){
        while(!states.isEmpty()) {
            ResolutionState state = states.pop();

            System.out.printf("%s\n", state);

            if (state.isAnswerState() && state.isTopState()) {
                return state.getSubstitution();
            }

            ResolutionState newState = state.generateChildState();
            if (newState != null) {
                if (LOG.isDebugEnabled()) logTree.addState(newState, state);

                if (!state.isAnswerState()) states.push(state);
                states.push(newState);
            } else {
                LOG.trace("new state: NULL");

                if (LOG.isDebugEnabled()) {
                    Node node = logTree.getNode(state);
                    if (node != null) node.ackCompletion();
                }

                if (state instanceof AtomicState) subGoalsCompleteOnFinalise.add(((AtomicState) state).getQuery());
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

    ResolutionTree getTree(){ return logTree;}

    private Boolean reiterate = null;

    private boolean reiterate(){
        if (reiterate == null) {
            reiterate = query.requiresReiteration();
        }
        return reiterate;
    }

    private long startTime = System.currentTimeMillis();
    private boolean resultsFinalised = false;

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
                LOG.debug("iter: {} answers: {} dAns = {} time = {}", iter, answers.size(), dAns, System.currentTimeMillis() - startTime);
                iter++;
                states.push(query.resolutionState(new ConceptMap(), new UnifierImpl(), null, new HashSet<>()));

                oldAns = answers.size();
                startTime = System.currentTimeMillis();
                return hasNext();
            }
        }

        if(!resultsFinalised) finalise();

        return false;
    }

    private void finalise(){
        MultilevelSemanticCache queryCache = CacheCasting.queryCacheCast(this.queryCache);
        subGoalsCompleteOnFinalise.forEach(queryCache::ackCompleteness);
        queryCache.propagateAnswers();

        if (LOG.isDebugEnabled()) logTree.outputToFile();
        resultsFinalised = true;
    }
}
