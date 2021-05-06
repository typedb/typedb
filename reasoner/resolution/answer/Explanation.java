/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.answer;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Explanation {

    private final Rule rule;
    private final Map<Retrievable, Set<Retrievable>> variableMapping;
    private final ConceptMap conclusionAnswer;
    private final ConceptMap conditionAnswer;
    private final int hash;

    public Explanation(Rule rule, Map<Retrievable, Set<Retrievable>> variableMapping, ConceptMap conclusionAnswer, ConceptMap conditionAnswer) {
        this.rule = rule;
        this.variableMapping = variableMapping;
        this.conclusionAnswer = conclusionAnswer;
        this.conditionAnswer = conditionAnswer;
        this.hash = Objects.hash(rule, variableMapping, conclusionAnswer, conditionAnswer);
    }

    public Rule rule() {
        return rule;
    }

    public Map<Retrievable, Set<Retrievable>> variableMapping() {
        return variableMapping;
    }

    public ConceptMap conclusionAnswer() {
        return conclusionAnswer;
    }

    public ConceptMap conditionAnswer() {
        return conditionAnswer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Explanation that = (Explanation) o;
        return Objects.equals(rule, that.rule) &&
                Objects.equals(variableMapping, that.variableMapping) &&
                Objects.equals(conclusionAnswer, that.conclusionAnswer) &&
                Objects.equals(conditionAnswer, that.conditionAnswer);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
