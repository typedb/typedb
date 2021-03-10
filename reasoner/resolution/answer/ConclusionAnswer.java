/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.answer;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;

public class ConclusionAnswer {

    private final Rule rule;
    private final ConceptMap conceptMap;
    private final Unifier unifier;
    private final ExplainableAnswer conditionAnswer;

    public ConclusionAnswer(Rule rule, ConceptMap conceptMap, Unifier unifier, ExplainableAnswer conditionAnswer) {
        this.rule = rule;
        this.conceptMap = conceptMap;
        this.unifier = unifier;
        this.conditionAnswer = conditionAnswer;
    }

    public Rule rule() {
        return rule;
    }

    public ExplainableAnswer conditionAnswer() {
        return conditionAnswer;
    }
}
