/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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

    public RuleExplanation(ReasonerQuery q, InferenceRule rl){
        super(q);
        this.rule = rl;
    }
    private RuleExplanation(ReasonerQuery q, List<Answer> answers, InferenceRule rl){
        super(q, answers);
        this.rule = rl;
    }

    @Override
    public AnswerExplanation setQuery(ReasonerQuery q){
        return new RuleExplanation(q, getRule());
    }

    @Override
    public AnswerExplanation childOf(Answer ans) {
        AnswerExplanation explanation = ans.getExplanation();
        return new RuleExplanation(getQuery(),
                ReasonerUtils.listUnion(this.getAnswers(), explanation.isLookupExplanation()?
                        Collections.singletonList(ans) :
                        explanation.getAnswers()),
                getRule());
    }

    @Override
    public boolean isRuleExplanation(){ return true;}

    public InferenceRule getRule(){ return rule;}
}
