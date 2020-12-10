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

package grakn.core.reasoner.resolution.answer;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.concludable.ConjunctionConcludable;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MappingAggregator extends Aggregator {

    private final Map<Reference.Name, Reference.Name> mapping;
    private final Map<Reference.Name, Reference.Name> reverseMapping;

    public static MappingAggregator of(ConceptMap conceptMap, Map<Reference.Name, Reference.Name> variableMap) {
        return new MappingAggregator(conceptMap, variableMap);
    }

    public static Map<Reference.Name, Reference.Name> identityMapping(ConjunctionConcludable<?, ?> concludable) {
        return new HashSet<>(concludable.constraint().variables()).stream()
                .filter(variable -> variable.reference().isName())
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    MappingAggregator(ConceptMap conceptMap, Map<Reference.Name, Reference.Name> mapping) {
        super(conceptMap, transform(conceptMap, mapping));
        this.mapping = mapping;
        this.reverseMapping = mapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    @Override
    ConceptMap unTransform(ConceptMap conceptMap) {
        return transform(conceptMap, reverseMapping);
    }

    private static ConceptMap transform(ConceptMap conceptMap, Map<Reference.Name, Reference.Name> mapping) {
        Map<Reference, Concept> transformed = new HashMap<>();
        for (Map.Entry<Reference.Name, Reference.Name> e : mapping.entrySet()) {
            transformed.put(e.getValue(), conceptMap.get(e.getKey()));
        }
        return new ConceptMap(transformed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MappingAggregator that = (MappingAggregator) o;
        return mapping.equals(that.mapping) &&
                Objects.equals(reverseMapping, that.reverseMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapping, reverseMapping);
    }
}
