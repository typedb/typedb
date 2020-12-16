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

public class Mapping extends VariableTransformer {

    private final Map<Reference.Name, Reference.Name> mapping;
    private final Map<Reference.Name, Reference.Name> reverseMapping;

    Mapping(Map<Reference.Name, Reference.Name> mapping) {
        this.mapping = mapping;
        this.reverseMapping = mapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public static Mapping of(ConceptMap conceptMap, Map<Reference.Name, Reference.Name> variableMap) {
        return new Mapping(variableMap);
    }

    public static Mapping of(Map<Reference.Name, Reference.Name> variableMap) {
        return new Mapping(variableMap);
    }

    public static Map<Reference.Name, Reference.Name> identity(ConjunctionConcludable<?, ?> concludable) {
        return new HashSet<>(concludable.constraint().variables()).stream()
                .filter(variable -> variable.reference().isName())
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Mapping that = (Mapping) o;
        return mapping.equals(that.mapping) &&
                Objects.equals(reverseMapping, that.reverseMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapping, reverseMapping);
    }

    public ConceptMap transform(ConceptMap toTransform) {
        return undirectedTransform(toTransform, mapping);
    }

//    @Override
    public ConceptMap unTransform(ConceptMap conceptMap) {
        return undirectedTransform(conceptMap, reverseMapping);
    }

    private static ConceptMap undirectedTransform(ConceptMap conceptMap, Map<Reference.Name, Reference.Name> mapping) {
        Map<Reference.Name, Concept> transformed = new HashMap<>();
        for (Map.Entry<Reference.Name, Reference.Name> e : mapping.entrySet()) {
            transformed.put(e.getValue(), conceptMap.get(e.getKey()));
        }
        return new ConceptMap(transformed);
    }

    @Override
    public boolean isMapping() {
        return true;
    }

    @Override
    public Mapping asMapped() {
        return this;
    }
}
