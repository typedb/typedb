package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by kasper on 31/05/17.
 */
public class ReasonerAtomicQueryCumulativeIterator extends ReasonerQueryIterator{
    private Answer partialSub = new QueryAnswer();

    private final LinkedList<ReasonerAtomicQuery> nextList;
    private final QueryCache<ReasonerAtomicQuery> cache;
    private final Set<ReasonerAtomicQuery> subGoals;

    private final Iterator<Answer> atomicQueryIterator;
    private Iterator<Answer> queryIterator;

    ReasonerAtomicQueryCumulativeIterator(Answer sub, LinkedList<ReasonerAtomicQuery> qs,
                                          Set<ReasonerAtomicQuery> subGoals,
                                          QueryCache<ReasonerAtomicQuery> cache){
        this.subGoals = subGoals;
        this.cache = cache;
        this.nextList = Lists.newLinkedList(qs);

        Iterator<Answer> iterator = nextList.removeFirst().iterator(sub, subGoals, cache);

        this.queryIterator = nextList.isEmpty()? iterator : Collections.emptyIterator();
        this.atomicQueryIterator = nextList.isEmpty()? Collections.emptyIterator() : iterator;
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;

        if (atomicQueryIterator.hasNext() && !nextList.isEmpty()) {
            partialSub = atomicQueryIterator.next();
            queryIterator = new ReasonerAtomicQueryCumulativeIterator(partialSub, nextList, subGoals, cache);
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
