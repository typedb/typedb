/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.explanation;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.kb.concept.api.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Explanation class for rule application.
 */
public class RuleExplanation extends Explanation {

    private final Rule rule;

    public RuleExplanation(Rule rule){
        this.rule = rule;
    }
    private RuleExplanation(List<ConceptMap> answers, Rule rule){
        super(answers);
        this.rule = rule;
    }

    @Override
    public RuleExplanation childOf(ConceptMap ans) {
        Explanation explanation = ans.explanation();
        List<ConceptMap> answerList = new ArrayList<>(this.getAnswers());
        answerList.addAll(
                explanation.isLookupExplanation()?
                        Collections.singletonList(ans) :
                        explanation.getAnswers()
        );
        return new RuleExplanation(answerList, rule);
    }

    @Override
    public boolean isRuleExplanation(){ return true;}

    public Rule getRule() {
        return rule;
    }
}
