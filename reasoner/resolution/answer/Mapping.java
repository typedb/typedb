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

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Mapping {

    private final Map<Reference.Name, Reference.Name> mapping;
    private final Map<Reference.Name, Reference.Name> reverseMapping;

    Mapping(Map<Reference.Name, Reference.Name> mapping) {
        this.mapping = mapping;
        this.reverseMapping = mapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public static Mapping of(Map<Reference.Name, Reference.Name> variableMap) {
        return new Mapping(variableMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mapping that = (Mapping) o;
        return mapping.equals(that.mapping) &&
                Objects.equals(reverseMapping, that.reverseMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapping, reverseMapping);
    }

    @Override
    public String toString() {
        return "Mapping{" +
                "mapping=" + mapping +
                '}';
    }

    public ConceptMap transform(ConceptMap conceptMap) {
        Map<Reference.Name, Concept> transformed = new HashMap<>();
        for (Map.Entry<Reference.Name, ? extends Concept> entry : conceptMap.concepts().entrySet()) {
            Reference.Name ref = entry.getKey();
            if (mapping.containsKey(ref)) {
                Concept concept = entry.getValue();
                transformed.put(mapping.get(ref), concept);
            }
        }
        return new ConceptMap(transformed);
    }

    public ConceptMap unTransform(ConceptMap conceptMap) {
        assert reverseMapping.size() == conceptMap.concepts().size();
        Map<Reference.Name, Concept> transformed = new HashMap<>();
        for (Map.Entry<Reference.Name, ? extends Concept> entry : conceptMap.concepts().entrySet()) {
            Reference.Name ref = entry.getKey();
            assert reverseMapping.containsKey(ref);
            Concept concept = entry.getValue();
            transformed.put(reverseMapping.get(ref), concept);
        }
        return new ConceptMap(transformed);
    }

}
