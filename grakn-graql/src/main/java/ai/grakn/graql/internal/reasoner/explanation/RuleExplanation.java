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

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
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
    public AnswerExplanation copy(){ return new RuleExplanation(this);}

    @Override
    public boolean isRuleExplanation(){ return true;}

    @Override
    public ReasonerQuery getQuery(){ return rule.getHead();}

    public InferenceRule getRule(){ return rule;}
}
