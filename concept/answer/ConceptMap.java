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

package grakn.core.concept.answer;

import grakn.core.concept.Concept;
import grakn.core.graph.vertex.Vertex;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.Map;

public class ConceptMap implements Answer {

    private final Map<Reference, ? extends Concept> concepts;

    public ConceptMap() {
        this(new HashMap<>());
    }

    public ConceptMap(Map<Reference, ? extends Concept> concepts) {
        this.concepts = concepts;
    }

    public static ConceptMap of(Map<Reference, Vertex<?, ?>> vertexMap) {
        return null; // TODO
    }

    public boolean contains(Reference variable) {
        return concepts.containsKey(variable);
    }

    public Concept get(Reference variable) {
        return concepts.get(variable);
    }

    public Map<Reference, ? extends Concept> concepts() { return concepts; }
}
