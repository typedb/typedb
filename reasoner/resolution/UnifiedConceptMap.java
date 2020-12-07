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

import grakn.core.concept.answer.ConceptMap;

import java.util.Objects;

public class UnifiedConceptMap { // TODO Actually implement ConceptMap interface
    private final ConceptMap source;
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
    }

    public Merged merge(ConceptMap unified) {
        return null; // TODO
    }

    public ConceptMap map() {
        // return unifiedConceptMap; // TODO Map this conceptmap using the unifier to give only the subset of variables the unifier transforms to
        return null;
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
        private final ConceptMap unifiedToMerge;

        Merged(ConceptMap unifiedToMerge) {
            this.unifiedToMerge = unifiedToMerge;
        }

        public ConceptMap unUnify() {
            return null; // TODO
        }
    }

}
