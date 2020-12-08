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

public class TransformedConceptMap {
    private final ConceptMap source;
    private final ConceptMap transformed;
    private final ConceptMapTransformer transformer;

    public static TransformedConceptMap of(ConceptMap conceptMap, Unifier unifier) {
        return new TransformedConceptMap(conceptMap, unifier);
    }

    public static TransformedConceptMap of(ConceptMap conceptMap, VariableMapper variableMapper) {
        return new TransformedConceptMap(conceptMap, variableMapper);
    }

    public static TransformedConceptMap empty() {
        return new TransformedConceptMap(new ConceptMap(), Unifier.identity());
    }

    TransformedConceptMap(ConceptMap source, ConceptMapTransformer transformer){
        this.source = source;
        this.transformer = transformer;
        transformed = transformer.transform(source);
    }

    public Merged merge(ConceptMap unified) {
        Map<Reference, Concept> mergedMap = new HashMap<>(this.transformed.concepts());
        mergedMap.putAll(unified.concepts());
        return new Merged(new ConceptMap(mergedMap));
    }

    public ConceptMap map() {
        return transformed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TransformedConceptMap that = (TransformedConceptMap) o;
        return source.equals(that.source) && transformer.equals(that.transformer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, transformer);
    }

    public class Merged {
        private final ConceptMap merged;

        Merged(ConceptMap merged) {
            this.merged = merged;
        }

        public ConceptMap unTransform() {
            return transformer.unTransform(merged);
        }
    }

}
