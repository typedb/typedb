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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import java.util.Collections;
import java.util.List;

/**
 *
 * <p>
 * Explanation class for rule application.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class RuleExplanation extends QueryExplanation {

    private final InferenceRule rule;

    public RuleExplanation(ReasonerQuery q, InferenceRule rl){
        super(q);
        this.rule = rl;
    }
    private RuleExplanation(ReasonerQuery q, List<ConceptMap> answers, InferenceRule rl){
        super(q, answers);
        this.rule = rl;
    }

    @Override
    public Explanation setQuery(ReasonerQuery q){
        return new RuleExplanation(q, getRule());
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        Explanation explanation = ans.explanation();
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
