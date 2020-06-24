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

package grakn.core.server.cache;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.kb.server.cache.ExplanationCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExplanationCacheImpl implements ExplanationCache {
    Map<AnswerEntry, Explanation> cache;

    public ExplanationCacheImpl() {
        cache = new HashMap<>();
    }

    /**
     * Record the explanation tree for a computed ConceptMap
     * @param answer
     * @param explanation
     */
    @Override
    public void record(ConceptMap answer, Explanation explanation) {
        cache.putIfAbsent(AnswerEntry.of(answer), explanation);
    }

    @Override
    public Explanation get(ConceptMap answer) {
        return cache.get(AnswerEntry.of(answer));
    }

    @Override
    public void clear() {
        cache.clear();
    }


    private static class AnswerEntry {
        private ConceptMap answer;

        private AnswerEntry(ConceptMap answer) {
            this.answer = answer;
        }

        public static AnswerEntry of(ConceptMap answer) {
            return new AnswerEntry(answer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(answer.map(), answer.getPattern());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || this.getClass() != obj.getClass()) return false;
            AnswerEntry that = (AnswerEntry) obj;
            return this.answer.map().equals(that.answer.map())
                    && this.answer.getPattern().equals(that.answer.getPattern());
        }
    }
}
