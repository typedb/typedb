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
import grakn.core.pattern.Conjunction;

import java.util.Objects;
import java.util.Set;

public class ExplainableAnswer {

    private final ConceptMap conceptMap;
    private final Conjunction conjunctionAnswered;
    private final Set<Conjunction> explainables;

    public ExplainableAnswer(ConceptMap conceptMap, Conjunction conjunctionAnswered, Set<Conjunction> explainables) {
        this.conceptMap = conceptMap;
        this.conjunctionAnswered = conjunctionAnswered;
        this.explainables = explainables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ExplainableAnswer that = (ExplainableAnswer) o;
        return Objects.equals(conceptMap, that.conceptMap) &&
                Objects.equals(conjunctionAnswered, that.conjunctionAnswered) &&
                Objects.equals(explainables, that.explainables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conceptMap, conjunctionAnswered, explainables);
    }
}
