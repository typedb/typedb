package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by kasper on 14/09/17.
 */
public class MultiAnswerState extends ResolutionState{
    private final Iterator<AnswerState> answerStateIterator;

    MultiAnswerState(Answer sub, Set<Unifier> mu, QueryState parent) {
        super(sub, parent);
        this.answerStateIterator = mu.stream()
                .map(u -> new AnswerState(getSubstitution(), u, getParentState()))
                .iterator();
    }

    @Override
    public ResolutionState generateSubGoal() {
        if (!answerStateIterator.hasNext()) return null;
        AnswerState state = answerStateIterator.next();
        return getParentState() != null? getParentState().propagateAnswer(state) : state;
    }
}
