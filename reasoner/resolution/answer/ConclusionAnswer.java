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
import grakn.core.concept.answer.ExplainableAnswer;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;

import java.util.Objects;

public class ConclusionAnswer {

    private final Rule rule;
    private final ConceptMap conceptMap;
    private final Unifier unifier;
    private final ExplainableAnswer conditionAnswer;
    private final int hash;

    public ConclusionAnswer(Rule rule, ConceptMap conceptMap, Unifier unifier, ExplainableAnswer conditionAnswer) {
        this.rule = rule;
        this.conceptMap = conceptMap;
        this.unifier = unifier;
        this.conditionAnswer = conditionAnswer;
        this.hash = Objects.hash(rule, conceptMap, unifier, conditionAnswer);
    }

    public Rule rule() {
        return rule;
    }

    public ExplainableAnswer conditionAnswer() {
        return conditionAnswer;
    }

    public ConceptMap answer() {
        return conceptMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConclusionAnswer that = (ConclusionAnswer) o;
        return Objects.equals(rule, that.rule) &&
                Objects.equals(conceptMap, that.conceptMap) &&
                Objects.equals(unifier, that.unifier) &&
                Objects.equals(conditionAnswer, that.conditionAnswer);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public Unifier unifier() {
        return unifier;
    }
}
