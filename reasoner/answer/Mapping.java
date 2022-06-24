/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.answer;

import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Mapping {

    private final Map<Retrievable, Retrievable> mapping;
    private final Map<Retrievable, Retrievable> reverseMapping;

    private Mapping(Map<? extends Retrievable, ? extends Retrievable> mapping) {
        this.mapping = new HashMap<>(mapping);
        this.reverseMapping = mapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public static Mapping of(Map<? extends Retrievable, ? extends Retrievable> variableMap) {
        return new Mapping(variableMap);
    }

    public ConceptMap transform(ConceptMap conceptMap) {
        Map<Retrievable, Concept> transformed = new HashMap<>();
        for (Map.Entry<Retrievable, ? extends Concept> entry : conceptMap.concepts().entrySet()) {
            Retrievable id = entry.getKey();
            Retrievable mapped = mapping.get(id);
            if (mapped != null) {
                Concept concept = entry.getValue();
                transformed.put(mapped, concept);
            }
        }
        return new ConceptMap(transformed);
    }

    public ConceptMap unTransform(ConceptMap conceptMap) {
        assert reverseMapping.size() >= conceptMap.concepts().size();
        Map<Retrievable, Concept> transformed = new HashMap<>();
        for (Map.Entry<Retrievable, ? extends Concept> entry : conceptMap.concepts().entrySet()) {
            Retrievable id = entry.getKey();
            assert reverseMapping.containsKey(id);
            Concept concept = entry.getValue();
            transformed.put(reverseMapping.get(id), concept);
        }
        return new ConceptMap(transformed, conceptMap.explainables()); // we ignore explainables because they can't be mapped here
    }

    private Map<Retrievable, Retrievable> mapping() {
        return mapping;
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

}
