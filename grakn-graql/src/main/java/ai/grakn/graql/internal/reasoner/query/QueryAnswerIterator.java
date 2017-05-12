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
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * Iterator for query answers maintaining the iterative behaviour of QSQ scheme.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class QueryAnswerIterator extends ReasonerQueryIterator {

    private int iter = 0;
    private long oldAns = 0;
    private final ReasonerQueryImpl query;
    private final Set<Answer> answers = new HashSet<>();

    private final QueryCache<ReasonerAtomicQuery> cache;
    private Iterator<Answer> answerIterator;

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerQueryImpl.class);

    QueryAnswerIterator(ReasonerQueryImpl q){
        this.query = q;
        this.cache = new QueryCache<>();
        LOG.trace(query.getResolutionPlan());
        this.answerIterator = query.iterator(new QueryAnswer(), new HashSet<>(), cache);
    }

    /**
     * check whether answers available, if answers not fully computed compute more answers
     * @return true if answers available
     */
    @Override
    public boolean hasNext() {
        if (answerIterator.hasNext()) return true;

        //iter finished
        long dAns = answers.size() - oldAns;
        if (dAns != 0 || iter == 0) {
            LOG.debug("iter: " + iter + " answers: " + answers.size() + " dAns = " + dAns);
            iter++;
            answerIterator = query.iterator(new QueryAnswer(), new HashSet<>(), cache);
            oldAns = answers.size();
            return answerIterator.hasNext();
        }
        else return false;
    }

    /**
     * @return single answer to the query
     */
    @Override
    public Answer next() {
        Answer ans = answerIterator.next();
        answers.add(ans);
        return ans;
    }
}
