package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

/**
 * Created by kasper on 08/11/17.
 */
public class RuleState extends ConjunctiveState{

    private final InferenceRule rule;

    public RuleState(QueryState con, InferenceRule rule) {
        super(con.getQuery(), con.getSubstitution(), con.getUnifier(), con.getParentState(), con.getVisitedSubGoals(), con.getCache());
        this.rule = rule;
    }

    public InferenceRule getRule(){ return rule;}

    @Override
    public boolean isRuleState(){ return true;}

    @Override
    ResolutionState propagateAnswer(AnswerState state){
        Answer answer = state.getAnswer();
        return !answer.isEmpty()? new AnswerState(answer, getUnifier(), getParentState(), rule) : null;
    }

}
