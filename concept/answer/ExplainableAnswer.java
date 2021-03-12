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

package grakn.core.concept.answer;

import grakn.core.pattern.Conjunction;

import java.util.Objects;
import java.util.Set;

public class ExplainableAnswer {

    private final ConceptMap completeMap;
    private final Conjunction conjunctionAnswered;
    private final Set<Explainable> explainables;

    private final int hash;

    public ExplainableAnswer(ConceptMap completeMap, Conjunction conjunctionAnswered, Set<Explainable> explainables) {
        this.completeMap = completeMap;
        this.conjunctionAnswered = conjunctionAnswered;
        this.explainables = explainables;
        this.hash = Objects.hash(completeMap, conjunctionAnswered, explainables);
    }

    public ConceptMap completeMap() {
        return completeMap;
    }

    public Conjunction conjunctionAnswered() {
        return conjunctionAnswered;
    }

    public Set<Explainable> explainables() {
        return explainables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ExplainableAnswer that = (ExplainableAnswer) o;
        return Objects.equals(completeMap, that.completeMap) &&
                Objects.equals(conjunctionAnswered, that.conjunctionAnswered) &&
                Objects.equals(explainables, that.explainables);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class Explainable {

        public static long NOT_IDENTIFIED = -1L;

        private final Conjunction conjunction;
        private long explainableId;

        private Explainable(Conjunction conjunction, long explainableId) {
            this.conjunction = conjunction;
            this.explainableId = explainableId;
        }

        public static Explainable unidentified(Conjunction conjunction) {
            return new Explainable(conjunction, NOT_IDENTIFIED);
        }

        public void withId(long explainableId) {
            this.explainableId = explainableId;
        }

        public Conjunction conjunction() {
            return conjunction;
        }

        public long explainableId() {
            return explainableId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Explainable that = (Explainable) o;
            return explainableId == that.explainableId &&
                    Objects.equals(conjunction, that.conjunction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conjunction, explainableId);
        }
    }
}
