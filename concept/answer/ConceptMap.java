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
 *
 */

package grakn.core.concept.answer;

import grakn.core.concept.Concept;
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.UnboundVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConceptMap implements Answer {

    private final Map<Reference.Name, ? extends Concept> concepts;
    private final int hash;

    public ConceptMap() {
        this(new HashMap<>());
    }

    public ConceptMap(Map<Reference.Name, ? extends Concept> concepts) {
        this.concepts = concepts;
        this.hash = Objects.hash(this.concepts);
    }

    public boolean contains(String variable) {
        return contains(Reference.name(variable));
    }

    public boolean contains(Reference.Name variable) {
        return concepts.containsKey(variable);
    }

    public Concept get(String variable) {
        return get(Reference.name(variable));
    }

    public Concept get(UnboundVariable variable) {
        if (!variable.reference().isName()) return null;
        else return get(variable.reference().asName());
    }

    public Concept get(Reference.Name variable) {
        return concepts.get(variable);
    }

    public Map<Reference.Name, ? extends Concept> concepts() { return concepts; }

    public ConceptMap filter(Set<Reference.Name> vars) {
        Map<Reference.Name, ? extends Concept> filtered = concepts.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new ConceptMap(filtered);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConceptMap that = (ConceptMap) o;
        return concepts.equals(that.concepts);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "ConceptMap{" + concepts + '}';
    }
}
