package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import java.util.Set;

/**
 * Created by kasper on 14/09/17.
 */
class AnswerStateFactory {

    static ResolutionState create(Answer sub, Set<Unifier> mu, QueryState parent){
        if(sub.isEmpty()) return null;
        return mu.size() > 1?
                new MultiAnswerState(sub, mu, parent) :
                new AnswerState(sub, mu.iterator().next(), parent);
    }
}
