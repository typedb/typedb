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

package grakn.core.graql.reasoner.explanation;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import graql.lang.pattern.Pattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Explanation class for rule application.
 */
public class RuleExplanation extends Explanation {

    private final String ruleId;

    public RuleExplanation(Pattern pattern, String ruleId){
        super(pattern);
        this.ruleId = ruleId;
    }
    private RuleExplanation(Pattern queryPattern, List<ConceptMap> answers, String ruleId){
        super(queryPattern, answers);
        this.ruleId = ruleId;
    }

    @Override
    public RuleExplanation setPattern(Pattern pattern){
        return new RuleExplanation(pattern, getRuleId());
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
        return new RuleExplanation(getPattern(), answerList, getRuleId());
    }

    @Override
    public boolean isRuleExplanation(){ return true;}

    public String getRuleId(){ return ruleId;}
}
