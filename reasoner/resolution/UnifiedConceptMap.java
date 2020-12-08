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
 */

package grakn.core.reasoner.resolution;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UnifiedConceptMap {
    private final ConceptMap source;
    private final ConceptMap unified;
    private final Unifier unifier; // Needs to be a Set of Pairs, or a Map<Variable, Set<Variable>>

    public static UnifiedConceptMap of(ConceptMap conceptMap, Unifier unifier) {
        return new UnifiedConceptMap(conceptMap, unifier);
    }

    public static UnifiedConceptMap empty() {
        return new UnifiedConceptMap(new ConceptMap(), Unifier.identity());
    }

    UnifiedConceptMap(ConceptMap source, Unifier unifier){
        this.source = source;
        this.unifier = unifier;
        unified = unifier.unify(source);
    }

    public Merged merge(ConceptMap unified) {
        Map<Reference, Concept> mergedMap = new HashMap<>(this.unified.concepts());
        mergedMap.putAll(unified.concepts());
        return new Merged(new ConceptMap(mergedMap));
    }

    public ConceptMap map() {
        return unified;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final UnifiedConceptMap that = (UnifiedConceptMap) o;
        return source.equals(that.source) && unifier.equals(that.unifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, unifier);
    }

    public class Merged {
        private final ConceptMap merged;

        Merged(ConceptMap merged) {
            this.merged = merged;
        }

        public ConceptMap unUnify() {
            return unifier.unUnify(merged);
        }
    }

}
