/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.reasoner.explanation;

import grakn.core.graql.admin.Explanation;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.reasoner.rule.InferenceRule;
import grakn.core.graql.internal.reasoner.utils.ReasonerUtils;
import java.util.Collections;
import java.util.List;

/**
 *
 * <p>
 * Explanation class for rule application.
 * </p>
 *
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
