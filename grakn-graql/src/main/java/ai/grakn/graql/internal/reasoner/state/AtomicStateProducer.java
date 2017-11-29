package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by kasper on 29/11/17.
 */

public class AtomicStateProducer extends QueryStateBase {

    private final Iterator<ResolutionState> subGoalIterator;

    public AtomicStateProducer(ReasonerAtomicQuery query, Answer sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache) {
        super(sub, u, parent, subGoals, cache);
        this.subGoalIterator = query.subGoals(sub, u, parent, subGoals, cache).iterator();
    }

    @Override
    public ResolutionState generateSubGoal() {
        return subGoalIterator.hasNext() ? subGoalIterator.next() : null;
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        return new AnswerState(state.getSubstitution(), state.getUnifier(), getParentState());
    }

    @Override
    Answer consumeAnswer(AnswerState state) {
        return state.getSubstitution();
    }
}

