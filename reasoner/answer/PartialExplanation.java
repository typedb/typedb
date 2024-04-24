/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.answer;

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Map;
import java.util.Objects;

public class PartialExplanation {

    private final Rule rule;
    private final ConclusionAnswer conclusionAnswer;
    private final ConceptMap conditionAnswer;
    private final int hash;

    PartialExplanation(Rule rule, ConclusionAnswer conclusionAnswer, ConceptMap conditionAnswer) {
        this.rule = rule;
        this.conclusionAnswer = conclusionAnswer;
        this.conditionAnswer = conditionAnswer;
        this.hash = Objects.hash(rule, conclusionAnswer, conditionAnswer);
    }

    public static PartialExplanation create(Rule rule, Map<Identifier.Variable, Concept> conclusionAnswer, ConceptMap conditionAnswer) {
        return new PartialExplanation(rule, new ConclusionAnswer(conclusionAnswer), conditionAnswer);
    }

    public Rule rule() {
        return rule;
    }

    public ConclusionAnswer conclusionAnswer() {
        return conclusionAnswer;
    }

    public ConceptMap conditionAnswer() {
        return conditionAnswer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PartialExplanation that = (PartialExplanation) o;
        return Objects.equals(rule, that.rule) &&
                Objects.equals(conclusionAnswer, that.conclusionAnswer) &&
                Objects.equals(conditionAnswer, that.conditionAnswer);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class ConclusionAnswer {

        private final Map<Identifier.Variable, Concept> concepts;

        private ConclusionAnswer(Map<Identifier.Variable, Concept> concepts) {
            this.concepts = concepts;
        }

        public Map<Identifier.Variable, Concept> concepts() {
            return concepts;
        }
    }
}
