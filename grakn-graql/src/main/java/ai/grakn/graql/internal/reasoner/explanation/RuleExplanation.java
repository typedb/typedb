package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;

/**
 *
 * <p>
 * Explanation class for rule application.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RuleExplanation extends Explanation {

    private final InferenceRule rule;

    public RuleExplanation(InferenceRule rl){ this.rule = rl;}
    public RuleExplanation(RuleExplanation exp){
        super(exp);
        this.rule = exp.getRule();
    }

    @Override
    public Explanation copy(){ return new RuleExplanation(this);}

    public InferenceRule getRule(){ return rule;}

    @Override
    public boolean isRuleExplanation(){ return true;}
}
