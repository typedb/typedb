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

import grakn.core.concept.answer.ExplainableAnswer;
import grakn.core.logic.Rule;
import grakn.core.traversal.common.Identifier;

import java.util.Map;
import java.util.Set;

public class Explanation {

    private final Rule rule;
    private final Mapping intermediateMapping;
    private final ConclusionAnswer conclusionAnswer;
    private final ExplainableAnswer conditionAnswer;

    public Explanation(Rule rule, Mapping intermediateMapping, ConclusionAnswer conclusionAnswer, ExplainableAnswer conditionAnswer) {
        this.rule = rule;
        this.intermediateMapping = intermediateMapping;
        this.conclusionAnswer = conclusionAnswer;
        this.conditionAnswer = conditionAnswer;
    }

    public Map<Identifier.Variable.Retrievable, Set<Identifier.Variable.Retrievable>> variableMapping() {
        // TODO merge mapping and conclusionAnswer.unifier
        return null;
    }

}
