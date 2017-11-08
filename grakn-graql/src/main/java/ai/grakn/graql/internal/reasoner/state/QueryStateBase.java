package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import java.util.Set;

/**
 * Created by kasper on 08/11/17.
 */
public abstract class QueryStateBase extends ResolutionState {

    private final Unifier unifier;
    private final Set<ReasonerAtomicQuery> visitedSubGoals;
    private final QueryCache<ReasonerAtomicQuery> cache;

    QueryStateBase(Answer sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, parent);
        this.unifier = u;
        this.visitedSubGoals = subGoals;
        this.cache = cache;
    }

    /**
     * @return true if this state corresponds to an atomic state
     */
    boolean isAtomicState(){ return false; }

    boolean isRuleState(){ return false; }

    /**
     * @return set of already visited subGoals (atomic queries)
     */
    Set<ReasonerAtomicQuery> getVisitedSubGoals(){ return visitedSubGoals;}

    /**
     * @return query cache
     */
    QueryCache<ReasonerAtomicQuery> getCache(){ return cache;}

    /**
     * @return unifier of this state with parent state
     */
    public Unifier getUnifier(){ return unifier;}

    /**
     * propagates the answer state up the tree and acknowledges (caches) its substitution
     * @param state to propagate
     * @return new resolution state obtained by propagating the answer up the resolution tree
     */
    abstract ResolutionState propagateAnswer(AnswerState state);
}
