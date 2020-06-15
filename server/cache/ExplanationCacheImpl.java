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

public class ExplanationCacheImpl implements ExplanationCache {
    Map<ConceptMap, Explanation> cache;

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
        cache.putIfAbsent(answer, explanation);
    }

    @Override
    public Explanation get(ConceptMap answer) {
        return cache.get(answer);
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
