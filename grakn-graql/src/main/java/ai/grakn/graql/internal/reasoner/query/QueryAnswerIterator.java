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
        else {
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
