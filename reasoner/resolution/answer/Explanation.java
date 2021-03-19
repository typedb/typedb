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
import grakn.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Explanation {

    private final Rule rule;
    private final Mapping intermediateMapping;
    private final ConclusionAnswer conclusionAnswer;
    private final ConceptMap conditionAnswer;
    private final int hash;

    public Explanation(Rule rule, Mapping intermediateMapping, ConclusionAnswer conclusionAnswer, ConceptMap conditionAnswer) {
        this.rule = rule;
        this.intermediateMapping = intermediateMapping;
        this.conclusionAnswer = conclusionAnswer;
        this.conditionAnswer = conditionAnswer;
        this.hash = Objects.hash(rule, intermediateMapping, conclusionAnswer, conditionAnswer);
    }

    public Map<Retrievable, Set<Retrievable>> variableMapping() {
        Unifier unifier = conclusionAnswer.unifier();
        Map<Retrievable, Set<Retrievable>> merged = new HashMap<>();

        intermediateMapping.mapping().forEach((from, to) -> {
            Set<Retrievable> tos = merged.computeIfAbsent(from, (key) -> new HashSet<>());
            if (unifier.mapping().containsKey(to)) {
                unifier.mapping().get(to).forEach(var -> {
                    if (var.isRetrievable()) tos.add(var.asRetrievable());
                });
            }
        });
        return merged;
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
        final Explanation that = (Explanation) o;
        return Objects.equals(rule, that.rule) &&
                Objects.equals(intermediateMapping, that.intermediateMapping) &&
                Objects.equals(conclusionAnswer, that.conclusionAnswer) &&
                Objects.equals(conditionAnswer, that.conditionAnswer);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
