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
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.state.ResolutionState;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ResolutionIterator extends ReasonerQueryIterator {

    private int iter = 0;
    private long oldAns = 0;
    private final ReasonerQueryImpl query;
    private final Set<Answer> answers = new HashSet<>();

    private final QueryCache<ReasonerAtomicQuery> cache;

    private ResolutionState answerState;
    private final Stack<ResolutionState> states = new Stack<>();

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    ResolutionIterator(ReasonerQueryImpl q){
        this.query = q;
        this.cache = new QueryCache<>();
        states.push(query.subGoal(new QueryAnswer(), new UnifierImpl(), null, new HashSet<>(), cache));
    }

    private ResolutionState findNextAnswerState(){
        while(!states.isEmpty()) {
            ResolutionState state = states.pop();

            if (state.isAnswerState() && state.isTopState()) {
                return state;
            }

            ResolutionState newState = state.generateSubGoal();
            if (newState != null) {
                if (!state.isAnswerState()) states.push(state);
                states.push(newState);
            }
        }
        return null;
    }

    @Override
    public Answer next(){
        Answer ans = answerState.getSubstitution();
        answers.add(ans);
        return ans;
    }

    /**
     * check whether answers available, if answers not fully computed compute more answers
     * @return true if answers available
     */
    @Override
    public boolean hasNext() {
        answerState = findNextAnswerState();
        if (answerState != null) return true;

        //iter finished
        long dAns = answers.size() - oldAns;
        if (dAns != 0 || iter == 0) {
            LOG.debug("iter: " + iter + " answers: " + answers.size() + " dAns = " + dAns);
            iter++;
            states.push(query.subGoal(new QueryAnswer(), new UnifierImpl(), null, new HashSet<>(), cache));
            oldAns = answers.size();
            return hasNext();
        }
        else return false;
    }
}
